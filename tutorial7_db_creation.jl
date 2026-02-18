TulipaIO.read_csv_folder(
    connection,
    user_input_dir,
    replace_if_exists = true,
)

TulipaClustering.transform_wide_to_long!(connection, "profiles", "pivot_profiles")

DuckDB.query(
    connection,
    "CREATE OR REPLACE TABLE profiles AS
    FROM pivot_profiles
    ORDER BY profile_name, year, timestep
    "
)

clustering_params = (
    num_rep_periods = 3,    # number of representative periods
    period_duration = 24,   # hours of the representative period
    method = :k_means,
    distance = SqEuclidean(),
    ## Data for weight fitting
    weight_type = :convex,
    tol = 1e-2,
)

Random.seed!(123)
TulipaClustering.cluster!(
    connection,
    clustering_params.period_duration,  # Required
    clustering_params.num_rep_periods;  # Required
    clustering_params.method,           # Optional
    clustering_params.distance,         # Optional
    clustering_params.weight_type,      # Optional
    clustering_params.tol,              # Optional
);

# assets
DuckDB.query(
    connection,
    "CREATE TABLE asset AS
    SELECT
        name AS asset,
        type,
        capacity,
        capacity_storage_energy,
        is_seasonal,
    FROM (
        FROM assets_consumer_basic_data
        UNION BY NAME
        FROM assets_conversion_basic_data
        UNION BY NAME
        FROM assets_hub_basic_data
        UNION BY NAME
        FROM assets_producer_basic_data
        UNION BY NAME
        FROM assets_storage_basic_data
    )
    ORDER BY asset
    ",
)

DuckDB.query(
    connection,
    "CREATE TABLE t_asset_yearly AS
    FROM (
        FROM assets_consumer_yearly_data
        UNION BY NAME
        FROM assets_conversion_yearly_data
        UNION BY NAME
        FROM assets_hub_yearly_data
        UNION BY NAME
        FROM assets_producer_yearly_data
        UNION BY NAME
        FROM assets_storage_yearly_data
    )
    ",
)

DuckDB.query(
    connection,
    "CREATE TABLE asset_commission AS
    SELECT
        name AS asset,
        year AS commission_year,
    FROM t_asset_yearly
    ORDER by asset
    "
)

DuckDB.query(
    connection,
    "CREATE TABLE asset_milestone AS
    SELECT
        name AS asset,
        year AS milestone_year,
        peak_demand,
        initial_storage_level,
        storage_inflows,
    FROM t_asset_yearly
    ORDER by asset
    "
)

DuckDB.query(
    connection,
    "CREATE TABLE asset_both AS
    SELECT
        name AS asset,
        year AS milestone_year,
        year AS commission_year, -- Yes, it is the same year twice with different names because it's not a multi-year problem
        initial_units,
        initial_storage_units,
    FROM t_asset_yearly
    ORDER by asset
    "
)

# flows
DuckDB.query(
    connection,
    "CREATE TABLE flow AS
    SELECT
        from_asset,
        to_asset,
        carrier,
        capacity,
        is_transport,
    FROM (
        FROM flows_assets_connections_basic_data
        UNION BY NAME
        FROM flows_transport_assets_basic_data
    )
    ORDER BY from_asset, to_asset
    ",
)

DuckDB.query(
    connection,
    "CREATE TABLE t_flow_yearly AS
    FROM (
        FROM flows_assets_connections_yearly_data
        UNION BY NAME
        FROM flows_transport_assets_yearly_data
    )
    ",
)

DuckDB.query(
    connection,
    "CREATE TABLE flow_commission AS
    SELECT
        from_asset,
        to_asset,
        year AS commission_year,
        efficiency AS producer_efficiency,
    FROM t_flow_yearly
    ORDER by from_asset, to_asset
    "
)

DuckDB.query(
    connection,
    "CREATE TABLE flow_milestone AS
    SELECT
        from_asset,
        to_asset,
        year AS milestone_year,
        variable_cost AS operational_cost,
    FROM t_flow_yearly
    ORDER by from_asset, to_asset
    "
)

DuckDB.query(
    connection,
    "CREATE TABLE flow_both AS
    SELECT
        t_flow_yearly.from_asset,
        t_flow_yearly.to_asset,
        t_flow_yearly.year AS milestone_year,
        t_flow_yearly.year AS commission_year,
        t_flow_yearly.initial_export_units,
        t_flow_yearly.initial_import_units,
    FROM t_flow_yearly
    LEFT JOIN flow
      ON flow.from_asset = t_flow_yearly.from_asset
      AND flow.to_asset = t_flow_yearly.to_asset
    WHERE flow.is_transport = TRUE -- flow_both must only contain transport flows
    ORDER by t_flow_yearly.from_asset, t_flow_yearly.to_asset
    "
)

# profiles
DuckDB.query(
    connection,
      "CREATE TABLE assets_timeframe_profiles AS
      SELECT
        asset,
        commission_year AS year,
        profile_type,
        profile_name
      FROM assets_storage_min_max_reservoir_level_profiles
      ORDER BY asset, year, profile_name
      ",
)

# asset partitions
DuckDB.query(
    connection,
    "CREATE TABLE assets_rep_periods_partitions AS
    SELECT
        t.name AS asset,
        t.year,
        t.partition AS partition,
        rep_periods_data.rep_period,
        'uniform' AS specification,
    FROM t_asset_yearly AS t
    LEFT JOIN rep_periods_data
        ON t.year = rep_periods_data.year
    ORDER BY asset, t.year, rep_period
    ",
)

# flow partitions
DuckDB.query(
    connection,
    "CREATE TABLE flows_rep_periods_partitions AS
    SELECT
        flow.from_asset,
        flow.to_asset,
        t_from.year,
        t_from.rep_period,
        'uniform' AS specification,
        IF(
            flow.is_transport,
            greatest(t_from.partition::int, t_to.partition::int),
            least(t_from.partition::int, t_to.partition::int)
        ) AS partition,
    FROM flow
    LEFT JOIN assets_rep_periods_partitions AS t_from
        ON flow.from_asset = t_from.asset
    LEFT JOIN assets_rep_periods_partitions AS t_to
        ON flow.to_asset = t_to.asset
        AND t_from.year = t_to.year
        AND t_from.rep_period = t_to.rep_period
    ",
)

# timeframe profiles
TulipaClustering.transform_wide_to_long!(
    connection,
    "min_max_reservoir_levels",
    "pivot_min_max_reservoir_levels",
)

period_duration = clustering_params.period_duration

DuckDB.query(
    connection,
    "
    CREATE TABLE profiles_timeframe AS
    WITH cte_split_profiles AS (
        SELECT
            profile_name,
            year,
            1 + (timestep - 1) // $period_duration  AS period,
            1 + (timestep - 1)  % $period_duration AS timestep,
            value,
        FROM pivot_min_max_reservoir_levels
    )
    SELECT
        cte_split_profiles.profile_name,
        cte_split_profiles.year,
        cte_split_profiles.period,
        AVG(cte_split_profiles.value) AS value, -- Computing the average aggregation
    FROM cte_split_profiles
    GROUP BY
        cte_split_profiles.profile_name,
        cte_split_profiles.year,
        cte_split_profiles.period
    ORDER BY
        cte_split_profiles.profile_name,
        cte_split_profiles.year,
        cte_split_profiles.period
    ",
)