using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame

source_db = "db_files/base_db.db"

for (name, alpha) in [("low_var", 0.5), ("high_var", 2.0)]
    target_db = "db_files/$(name).db"
    cp(source_db, target_db, force=true)

    connection = DBInterface.connect(DuckDB.DB, target_db)

    n_rep_periods = DBInterface.execute(connection,
        "SELECT COUNT(DISTINCT rep_period) FROM profiles_rep_periods"
    ) |> DataFrame
    @assert n_rep_periods[1,1] == 1 "Expected exactly 1 rep_period, got $(n_rep_periods[1,1])"

    DuckDB.query(connection, """
        CREATE OR REPLACE TABLE profiles_rep_periods AS
        WITH means AS (
            SELECT
                profile_name,
                AVG(value) AS mean_value
            FROM profiles_rep_periods
            GROUP BY profile_name
        ),
        scaled AS (
            SELECT
                p.rep_period,
                p.timestep,
                p.year,
                p.profile_name,
                CASE
                    WHEN p.profile_name LIKE '%Demand%'
                      OR p.profile_name LIKE '%Wind%'
                      OR p.profile_name LIKE '%Solar%'
                    THEN GREATEST(0.0, LEAST(1.0,
                            $alpha * (p.value - m.mean_value) + m.mean_value
                         ))
                    ELSE p.value
                END AS value
            FROM profiles_rep_periods p
            JOIN means m ON p.profile_name = m.profile_name
        )
        SELECT * FROM scaled
    """)

    DBInterface.close!(connection)
    println("Created $target_db with alpha=$alpha")
end