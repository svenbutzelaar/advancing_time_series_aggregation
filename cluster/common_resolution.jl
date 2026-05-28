"""
    common_highest_resolution(partitions::Vector{Vector{Int}}) -> Vector{Int}

Given a collection of partitions (each a vector of block sizes that sum to the
same total number of timesteps), return the coarsest partition that is a
refinement of *all* of them — i.e. the one whose split-point set is the union
of every individual split-point set.

Example
-------
    common_highest_resolution([[4, 2], [2, 4]])  # => [2, 2, 2]
    common_highest_resolution([[3, 3], [2, 4]])  # => [2, 1, 3]
"""
function common_highest_resolution(partitions::Vector{Vector{Int}})::Vector{Int}
    isempty(partitions) && return Int[]

    # Collect all split points (cumulative sums, *excluding* the final total)
    split_points = Set{Int}()
    total = sum(first(partitions))
    for p in partitions
        cumsum_val = 0
        for block in p[1:end-1]          # last block has no split after it
            cumsum_val += block
            push!(split_points, cumsum_val)
        end
    end

    # Reconstruct block sizes from sorted split points
    sorted_splits = sort!(collect(split_points))
    blocks = Int[]
    prev = 0
    for s in sorted_splits
        push!(blocks, s - prev)
        prev = s
    end
    push!(blocks, total - prev)          # final block
    return blocks
end


"""
    compute_location_common_resolutions(results::DataFrame)
        -> Dict{Tuple{Int,Int,String}, Vector{Int}}

For every (rep_period, year, location) group in `results`, compute the common
highest resolution across all profiled assets at that location.  Returns a
dictionary keyed by (rep_period, year, location).
"""
function compute_location_common_resolutions(
    results::DataFrame,
)::Dict{Tuple{Int,Int,String}, Vector{Int}}

    common = Dict{Tuple{Int,Int,String}, Vector{Int}}()

    for g in groupby(results, [:rep_period, :year, :location])
        rep_period = first(g.rep_period)
        year       = first(g.year)
        location   = first(g.location)
        key        = (rep_period, year, location)

        # Parse each asset's partition string into a Vector{Int}
        partitions = [
            parse.(Int, split(row.partition, ";"))
            for row in eachrow(g)
        ]

        common[key] = common_highest_resolution(partitions)
    end

    return common
end


"""
    update_non_profiled_assets_and_flows!(conn, results, common_resolutions)

1. For every asset without time profiles, set their partition to the common 
   highest resolution of their location.
2. For flows where one endpoint is a profiled asset (demand, wind, solar),
   use that asset's own partition directly — so e.g. the demand→balance flow
   inherits demand's partition rather than the location common resolution.
   When both endpoints are profiled, take the common highest resolution of the
   two asset partitions.
3. For intra-location flows where neither endpoint is profiled, use the
   location's common highest resolution.
4. For inter-location flows where neither endpoint is profiled, use the common
   highest resolution of the two locations' common resolutions.

The location of an asset is taken as the first two characters of its name,
matching the convention used in `cluster_partitions!`.
"""
function update_non_profiled_assets_and_flows!(
    conn,
    results::DataFrame,
    common_resolutions::Dict{Tuple{Int,Int,String}, Vector{Int}},
)
    # ── 1. Build the update rows for assets & flows ──────────────────────────

    asset_updates = DataFrame(
        asset         = String[],
        rep_period    = Int[],
        year          = Int[],
        partition     = String[],
        specification = String[],
    )

    flow_updates = DataFrame(
        from_asset    = String[],
        to_asset      = String[],
        rep_period    = Int[],
        year          = Int[],
        partition     = String[],
        specification = String[],
    )

    # ── Build a lookup: (asset, rep_period, year) -> partition Vector{Int} ────
    # This covers every asset that has a time-series profile (demand, wind, solar).
    profiled_partitions = Dict{Tuple{String,Int,Int}, Vector{Int}}()
    for row in eachrow(results)
        key = (row.asset, row.rep_period, row.year)
        profiled_partitions[key] = parse.(Int, split(row.partition, ";"))
    end

    # ── Build ENS → demand partition mapping ─────────────────────────────────
    # ENS node name: replace "_ENS" with "_Demand" (case-insensitive) to find
    # the corresponding demand asset's partition.
    ens_partitions = Dict{Tuple{String,Int,Int}, Vector{Int}}()
    for ((asset, rp, yr), partition) in profiled_partitions
        pt = getProfileType(asset)
        pt == ENS && continue  # ENS nodes shouldn't be in profiled, but guard anyway

        # For every demand asset, register the ENS counterpart
        if pt == Demand
            ens_name = replace(asset, r"(?i)demand" => "ENS")
            ens_partitions[(ens_name, rp, yr)] = partition
        end
    end

    # ── Fetch all assets that are NOT in the profiled results ────────────────
    non_profiled = DataFrame(DBInterface.execute(conn, """
        SELECT DISTINCT
            a.asset,
            a.rep_period,
            a.year,
            SUBSTRING(a.asset, 1, 2) AS location
        FROM assets_rep_periods_partitions AS a
        WHERE NOT EXISTS (
            SELECT 1
            FROM tmp_cluster_results AS r
            WHERE r.asset = a.asset
              AND r.rep_period = a.rep_period
              AND r.year = a.year
        )
    """))

    for row in eachrow(non_profiled)
        key = (row.rep_period, row.year, row.location)

        partition = if haskey(ens_partitions, (row.asset, row.rep_period, row.year))
            ens_partitions[(row.asset, row.rep_period, row.year)]
        elseif haskey(common_resolutions, key)
            common_resolutions[key]
        else
            continue
        end

        push!(asset_updates, (
            asset         = row.asset,
            rep_period    = row.rep_period,
            year          = row.year,
            partition     = join(partition, ";"),
            specification = "explicit",
        ))
    end

    # ── Fetch all flows ───────────────────────────────────────────────────────
    all_flows = DataFrame(DBInterface.execute(conn, """
        SELECT DISTINCT
            from_asset,
            to_asset,
            rep_period,
            year,
            SUBSTRING(from_asset, 1, 2) AS from_loc,
            SUBSTRING(to_asset,   1, 2) AS to_loc
        FROM flows_rep_periods_partitions
    """))

    for row in eachrow(all_flows)
        rp   = row.rep_period
        yr   = row.year
        floc = row.from_loc
        tloc = row.to_loc

        from_profiled_key = (row.from_asset, rp, yr)
        to_profiled_key   = (row.to_asset,   rp, yr)
        from_loc_key      = (rp, yr, floc)
        to_loc_key        = (rp, yr, tloc)

        from_profiled = haskey(profiled_partitions, from_profiled_key)
        to_profiled   = haskey(profiled_partitions, to_profiled_key)

        partition = if from_profiled && to_profiled
            # Both endpoints have profiles: take their common highest resolution
            common_highest_resolution([
                profiled_partitions[from_profiled_key],
                profiled_partitions[to_profiled_key],
            ])
        elseif from_profiled
            # Only from-asset is profiled (e.g. demand → balance node):
            # inherit that asset's partition directly
            profiled_partitions[from_profiled_key]
        elseif to_profiled
            # Only to-asset is profiled (e.g. balance node ← wind):
            # inherit that asset's partition directly
            profiled_partitions[to_profiled_key]
        elseif floc == tloc
            # Neither endpoint is profiled, intra-location: use location common
            haskey(common_resolutions, from_loc_key) || continue
            common_resolutions[from_loc_key]
        else
            # Neither endpoint is profiled, inter-location: common of both locations
            haskey(common_resolutions, from_loc_key) || continue
            haskey(common_resolutions, to_loc_key)   || continue
            common_highest_resolution([
                common_resolutions[from_loc_key],
                common_resolutions[to_loc_key],
            ])
        end

        push!(flow_updates, (
            from_asset    = row.from_asset,
            to_asset      = row.to_asset,
            rep_period    = rp,
            year          = yr,
            partition     = join(partition, ";"),
            specification = "explicit",
        ))
    end

    # ── 2. Write updates to the database ─────────────────────────────────────

    if nrow(asset_updates) > 0
        DuckDB.register_data_frame(conn, asset_updates, "tmp_nonprofiled_assets")
        DBInterface.execute(conn, """
            UPDATE assets_rep_periods_partitions AS a
            SET
                partition     = u.partition,
                specification = u.specification
            FROM tmp_nonprofiled_assets AS u
            WHERE
                a.asset      = u.asset
                AND a.rep_period = u.rep_period
                AND a.year   = u.year
        """)
        DBInterface.execute(conn, "DROP VIEW IF EXISTS tmp_nonprofiled_assets")
    end

    if nrow(flow_updates) > 0
        DuckDB.register_data_frame(conn, flow_updates, "tmp_flow_updates")
        DBInterface.execute(conn, """
            UPDATE flows_rep_periods_partitions AS f
            SET
                partition     = u.partition,
                specification = u.specification
            FROM tmp_flow_updates AS u
            WHERE
                f.from_asset = u.from_asset
                AND f.to_asset   = u.to_asset
                AND f.rep_period = u.rep_period
                AND f.year   = u.year
        """)
        DBInterface.execute(conn, "DROP VIEW IF EXISTS tmp_flow_updates")
    end
end