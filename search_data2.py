import pandas as pd

df = pd.read_csv('1h_old/asset.csv')

print(df["investment_cost"].unique())

nice_query("SELECT asset,investment_group,investment_integer,investment_method  FROM asset")

asset
 Row │ column_name                        data_type
     │ String                             String
─────┼──────────────────────────────────────────────
   1 │ asset                              VARCHAR
   2 │ type                               VARCHAR
   3 │ capacity                           DOUBLE
   4 │ capacity_storage_energy            DOUBLE
   5 │ is_seasonal                        BOOLEAN
   6 │ consumer_balance_sense             VARCHAR
   7 │ discount_rate                      DOUBLE
   8 │ economic_lifetime                  INTEGER
   9 │ energy_to_power_ratio              DOUBLE
  10 │ investment_group                   VARCHAR
  11 │ investment_integer                 BOOLEAN
  12 │ investment_integer_storage_energy  BOOLEAN
  13 │ investment_method                  VARCHAR
  14 │ max_ramp_down                      DOUBLE
  15 │ max_ramp_up                        DOUBLE
  16 │ min_operating_point                DOUBLE
  17 │ ramping                            BOOLEAN
  18 │ storage_method_energy              BOOLEAN
  19 │ technical_lifetime                 INTEGER
  20 │ unit_commitment                    BOOLEAN
  21 │ unit_commitment_integer            BOOLEAN
  22 │ unit_commitment_method             VARCHAR
  23 │ use_binary_storage_method          VARCHAR
  
asset_milestone
   Row │ column_name                     data_type
     │ String                          String
─────┼───────────────────────────────────────────
   1 │ asset                           VARCHAR
   2 │ milestone_year                  INTEGER
   3 │ peak_demand                     DOUBLE
   4 │ initial_storage_level           DOUBLE
   5 │ storage_inflows                 DOUBLE
   6 │ investable                      BOOLEAN
   7 │ max_energy_timeframe_partition  DOUBLE
   8 │ min_energy_timeframe_partition  DOUBLE
   9 │ units_on_cost                   DOUBLE
   
asset_both
 Row │ column_name            data_type
     │ String                 String
─────┼──────────────────────────────────
   1 │ asset                  VARCHAR
   2 │ milestone_year         INTEGER
   3 │ commission_year        INTEGER
   4 │ initial_units          DOUBLE
   5 │ initial_storage_units  DOUBLE
   6 │ decommissionable       BOOLEAN
   
asset_commission
 DataFrame
 Row │ column_name                      data_type
     │ String                           String
─────┼────────────────────────────────────────────
   1 │ asset                            VARCHAR
   2 │ commission_year                  INTEGER
   3 │ conversion_efficiency            DOUBLE
   4 │ fixed_cost                       DOUBLE
   5 │ fixed_cost_storage_energy        DOUBLE
   6 │ investment_cost                  DOUBLE
   7 │ investment_cost_storage_energy   DOUBLE
   8 │ investment_limit                 DOUBLE
   9 │ investment_limit_storage_energy  DOUBLE
  10 │ storage_charging_efficiency      DOUBLE
  11 │ storage_discharging_efficiency   DOUBLE
  12 │ storage_loss_from_stored_energy  DOUBL