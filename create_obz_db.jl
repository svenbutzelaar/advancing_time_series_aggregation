using Pkg
using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame
using TulipaIO: TulipaIO
using TulipaClustering: TulipaClustering
using TulipaEnergyModel: TulipaEnergyModel as TEM
using Distances: SqEuclidean
using Random: Random
include("cluster/cluster_partitions.jl")
include("cluster/config.jl")

user_input_dir = "../TulipaEnergyModel.jl/docs/src/data/obz/"

#parameters
num_rep_periods = 3
period_duration = 168

config = ClusteringConfig(
    dependant_per_location = true,
    extreme_preservation = NoExtremePreservation,
    high_percentile = 0.95,
    low_percentile = 0.05,
    )

num_clusters = 1500
    
file_name = experiment_name(config, num_clusters)
database_name = "$file_name.db"
    
readdir(user_input_dir)

if isfile(database_name)
    error("Database file '$database_name' already exists. Please remove it or use a different name.")
end

connection = DBInterface.connect(DuckDB.DB, database_name)

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
    num_rep_periods = 8760,    # number of representative periods
    period_duration = 1,   # hours of the representative period
    method = :k_means,
    distance = SqEuclidean(),
    ## Data for weight fitting
    weight_type = :convex,
    tol = 1e-2,
)

TulipaClustering.dummy_cluster!(
    connection,
);
DuckDB.query(connection, 
    "ALTER TABLE rep_periods_mapping
        ADD scenario Int32;",
)

# # Data for clustering
# clustering_params = (
#     num_rep_periods = num_rep_periods,    # number of representative periods
#     period_duration = period_duration,   # hours of the representative period
#     method = :k_means,
#     distance = SqEuclidean(),
#     ## Data for weight fitting
#     weight_type = :convex,
#     tol = 1e-2,
# )

# Random.seed!(123)
# TulipaClustering.cluster!(
#     connection,
#     clustering_params.period_duration,  # Required
#     clustering_params.num_rep_periods;  # Required
#     clustering_params.method,           # Optional
#     clustering_params.distance,         # Optional
#     clustering_params.weight_type,      # Optional
#     clustering_params.tol,              # Optional
# );



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
        CASE
            WHEN LOWER(name) LIKE '%wind_onshore%'
              OR LOWER(name) LIKE '%wind_offshore%'
              OR LOWER(name) LIKE '%solar%'
              OR LOWER(name) LIKE '%coal%'
              OR LOWER(name) LIKE '%ocgt%'
              OR LOWER(name) LIKE '%gas%'
              OR LOWER(name) LIKE '%nuclear%'
            THEN 'simple'
            ELSE 'none'
        END AS investment_method,
        false AS investment_integer
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

# investment_costs
# wind_onshore = 77356.32865703155
# wind_offshore = 119732.61777406993
# solar = 34342.98027538492
# batery = 77577.8503057667
# coal = 420000.0
# ocgt = 55000.0
# gas = 95000.0     
# nuclear = 950000.0
DuckDB.query(
    connection,
    "
    CREATE TABLE asset_commission AS
    SELECT
        tay.name AS asset,
        tay.year AS commission_year,
        CASE
            WHEN LOWER(tay.name) LIKE '%wind_onshore%'  THEN 77356.32865703155
            WHEN LOWER(tay.name) LIKE '%wind_offshore%' THEN 119732.61777406993
            WHEN LOWER(tay.name) LIKE '%solar%'          THEN 34342.98027538492
            WHEN LOWER(tay.name) LIKE '%coal%'           THEN 420000.0
            WHEN LOWER(tay.name) LIKE '%ocgt%'            THEN 55000.0
            WHEN LOWER(tay.name) LIKE '%gas%'             THEN 95000.0
            WHEN LOWER(tay.name) LIKE '%nuclear%'         THEN 950000.0
            ELSE NULL
        END AS investment_cost,
        2 * a.capacity AS investment_limit
    FROM t_asset_yearly tay
    JOIN asset a
    ON a.asset = tay.name
    ORDER BY asset;
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
        CASE
            WHEN LOWER(name) LIKE '%wind_onshore%'
              OR LOWER(name) LIKE '%wind_offshore%'
              OR LOWER(name) LIKE '%solar%'
              OR LOWER(name) LIKE '%coal%'
              OR LOWER(name) LIKE '%ocgt%'
              OR LOWER(name) LIKE '%gas%'
              OR LOWER(name) LIKE '%nuclear%'
            THEN true
            ELSE false
        END AS investable,
    FROM t_asset_yearly
    ORDER by asset
    "
)

DuckDB.query(
    connection,
    "
    CREATE TABLE asset_both AS
    SELECT
        tay.name AS asset,
        tay.year AS milestone_year,
        tay.year AS commission_year, -- same year, different semantic meaning
        CASE
            WHEN a.investment_method = 'simple' THEN 0
            ELSE tay.initial_units
        END AS initial_units,
        tay.initial_storage_units
    FROM t_asset_yearly tay
    JOIN asset a
    ON a.asset = tay.name
    ORDER BY asset;
    "
)

# TODO make flows investable

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
        t.partition::varchar(255) AS partition,
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
        )::varchar(255) AS partition,
    FROM flow
    LEFT JOIN assets_rep_periods_partitions AS t_from
        ON flow.from_asset = t_from.asset
    LEFT JOIN assets_rep_periods_partitions AS t_to
        ON flow.to_asset = t_to.asset
        AND t_from.year = t_to.year
        AND t_from.rep_period = t_to.rep_period
    ",
)

cluster_partitions!(connection, num_clusters, config)

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

TEM.populate_with_defaults!(connection)


close(connection)