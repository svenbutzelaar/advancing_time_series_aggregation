using DuckDB: DBInterface, DuckDB
using DataFrames
using CSV

# Connect to DuckDB (in-memory, or replace with a file DB if you want)
connection = DBInterface.connect(DuckDB.DB)

data_dir = "outputs-obz-invest-part-ens"   # change this later if needed

input_csv  = joinpath(data_dir, "var_flow.csv")
output_csv = joinpath(data_dir, "total_ens_per_country.csv")


# Create a view directly from the CSV
DuckDB.query(
    connection, """
CREATE OR REPLACE VIEW var_flow AS
SELECT *
FROM read_csv_auto('$input_csv');
""")

# Query total ENS per country
df = DataFrame(DuckDB.query(
    connection, """
SELECT
    substr(from_asset, 1, 2) AS country,
    SUM(solution) AS total_ens
FROM var_flow
WHERE from_asset LIKE '__\\_E_ENS%' ESCAPE '\\'
GROUP BY country
ORDER BY country;
"""))

CSV.write(output_csv, df)

close(connection)