function create_ens_db(config, energy_problem)
    file_name = experiment_name(config)
    target_db = "db_files/ens_$(file_name).db"
    source_db = "db_files/obz-invest-full-resolution.db"

    rm(target_db; force = true)
    cp(source_db, target_db; force = true)

    connection = DBInterface.connect(DuckDB.DB, target_db)

    DuckDB.query(connection, "
        UPDATE asset
        SET investment_method = 'none', investment_integer = false
    ")
    DuckDB.query(connection, "
        UPDATE asset_commission
        SET investment_cost = NULL, investment_limit = NULL
    ")
    DuckDB.query(connection, "
        UPDATE asset_milestone
        SET investable = false
    ")

    # Read investment solution directly from the solved model instead of CSV
    m = energy_problem.model
    var_assets_investment = TIO.get_table(energy_problem.connection, "var_assets_investment")
    solution_df = DataFrame(
        asset    = var_assets_investment.asset,
        solution = JuMP.value.(m[:assets_investment].container),
    )

    # Write to a temp view and update asset_both
    DuckDB.register_data_frame(connection, solution_df, "investment_solution_temp")
    DuckDB.query(connection, "
        UPDATE asset_both
        SET initial_units = v.solution
        FROM investment_solution_temp AS v
        WHERE asset_both.asset = v.asset
    ")

    close(connection)
    return target_db
end