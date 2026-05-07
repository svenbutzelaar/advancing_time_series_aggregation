# generate_uniform_sse.jl
# Computes SSE for uniform (equal-block) partitioning across all k values,
# producing a CSV in the same format as the Ward per_merge stats output.

using DuckDB: DBInterface, DuckDB
using DataFrames
using CSV
using Statistics: mean


# ----------------------------
# Settings — mirror your config
# ----------------------------
base_db = "db_files/base_db.db"

output_label = "uniform_NoExtremePreservation"   # used in output filename
dir          = "plotting/csv_data/per_merge"
mkpath(dir)
output_file  = "$dir/$output_label.csv"

# ----------------------------
# Connect & load profiles
# ----------------------------
connection = DBInterface.connect(DuckDB.DB, base_db)

df = DataFrame(DBInterface.execute(
    connection,
    """
    SELECT
        profile_name,
        SUBSTRING(profile_name, 1, 2) AS location,
        rep_period,
        timestep,
        year,
        value
    FROM profiles_rep_periods
    WHERE
            value IS NOT NULL
        AND NOT(LOWER(profile_name) LIKE '%hydro%')
    ORDER BY profile_name, rep_period, year, timestep
    """
))

# ----------------------------
# SSE for one uniform partition level
# Given a vector of T values and block_size b,
# SSE = sum over each block of sum((x_i - mean_block)^2)
# ----------------------------
function uniform_sse(values::Vector{Float64}, block_size::Int)::Float64
    T = length(values)
    sse = 0.0
    idx = 1
    while idx <= T
        block_end = min(idx + block_size - 1, T)
        block = values[idx:block_end]
        m = mean(block)
        sse += sum((v - m)^2 for v in block)
        idx += block_size
    end
    return sse
end

# ----------------------------
# Main loop: one row per (asset, rep_period, year)
# errors[i] = SSE vector over dimensions at merge step i
# merge step 1 = k=T (no merging, SSE=0), last step = k=1 (one block)
# We iterate k from T down to 1, matching Ward's merge order convention.
# ----------------------------
results = DataFrame(
    asset      = String[],
    location   = String[],
    rep_period = Int64[],
    year       = Int64[],
    errors     = Vector{Float64}[],
)

for key in groupby(df, [:profile_name, :location, :rep_period, :year])
    profile_name = key.profile_name[1]
    location     = key.location[1]
    rep_period   = key.rep_period[1]
    year         = key.year[1]

    sort!(key, :timestep)
    values = Float64.(key.value)
    T = length(values)

    # For each k (number of blocks), block_size = ceil(T/k)
    # k ranges from T (block_size=1, SSE=0) down to 1 (block_size=T)
    # This gives T merge steps, matching the Ward output length.
    sse_per_k = Float64[]
    for k in T:-1:1
        block_size = ceil(Int, T / k)
        if block_size == ceil(Int, T / k + 1)
             push!(sse_per_k, sse_per_k[-1])
        else
            push!(sse_per_k, uniform_sse(values, block_size))
        end
    end

    push!(results, (
        asset      = profile_name,
        location   = location,
        rep_period = rep_period,
        year       = year,
        errors     = sse_per_k,   # wrapped as 1-element vector of vectors (1 dimension)
    ))
end

# errors column must be a Vector{Float64} wrapped in a Vector (one entry per "dimension")
# to match the matrix format your Python code expects: matrix[merge_step, dimension]
# Since profiles are 1D, wrap each sse_per_k as a 1×n matrix row → store as [sse_per_k]
# i.e. each cell = "[0.0, 0.3, 1.2, ...]"

# Reformat: Python reads errors as list-of-lists (rows=assets, inner=merge steps, dim avg'd)
# Your Python does: matrix = array of rows, then mean over axis=0 (over assets/rep_periods)
# So each row's errors should be a flat vector of length n_merges = T-1 steps (one per k level)
# Already correct — just serialise as string

CSV.write(output_file, results)
println("Saved: $output_file  ($(nrow(results)) rows)")