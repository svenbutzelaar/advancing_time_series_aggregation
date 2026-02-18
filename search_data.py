import pandas as pd

# Load data from EU
eu_data = pd.read_csv('EU/profiles-rep-periods.csv')

# Load data from 1h_old
demand_data = pd.read_csv('1h_old/profiles-rep-periods-demand.csv', header=1)
availability_data = pd.read_csv('1h_old/profiles-rep-periods-availability.csv', header=1)

# Combine 1h_old data
old_data = pd.concat([demand_data, availability_data])

not_in_1h = {'AT_Hydro_Reservoir_Inflow', 'AT_Pump_Hydro_Open_Inflow', 'BG_Hydro_Reservoir_Inflow', 'BG_Pump_Hydro_Open_Inflow', 'CH_Pump_Hydro_Open_Inflow', 'CZ_Hydro_Reservoir_Inflow', 'CZ_Pump_Hydro_Open_Inflow', 'DE_Hydro_Reservoir_Inflow', 'DE_Pump_Hydro_Open_Inflow', 'ES_Hydro_Reservoir_Inflow', 'ES_Pump_Hydro_Open_Inflow', 'FI_Hydro_Reservoir_Inflow', 'FR_Hydro_Reservoir_Inflow', 'GR_Hydro_Reservoir_Inflow', 'GR_Pump_Hydro_Open_Inflow', 'HR_Hydro_Reservoir_Inflow', 'HR_Pump_Hydro_Open_Inflow', 'HR_Wind_Offshore', 'IT_Hydro_Reservoir_Inflow', 'IT_Pump_Hydro_Open_Inflow', 'NO_Pump_Hydro_Open_Inflow', 'OBZLL_E_Demand', 'OBZLL_Wind_Offshore', 'PL_Hydro_Reservoir_Inflow', 'PL_Pump_Hydro_Open_Inflow', 'PT_Hydro_Reservoir_Inflow', 'PT_Pump_Hydro_Open_Inflow', 'RO_Hydro_Reservoir_Inflow', 'RO_Pump_Hydro_Open_Inflow', 'RO_Wind_Offshore', 'SE_Hydro_Reservoir_Inflow', 'SI_Wind_Offshore', 'SK_Hydro_Reservoir_Inflow', 'SK_Pump_Hydro_Open_Inflow'}
not_in_eu = {'AT_H_Demand', 'BE_H_Demand', 'CZ_H_Demand', 'DE_H_Demand', 'DK_H_Demand', 'ES_H_Demand', 'FI_H_Demand', 'FR_H_Demand', 'GR_H_Demand', 'HU_H_Demand', 'IT_H_Demand', 'LU_H_Demand', 'NL_H_Demand', 'RO_H_Demand', 'SE_H_Demand'}

print(old_data.columns)
print(eu_data.columns)

old_data["year"] = 2050
old_data = old_data.rename(columns={'time_step': "timestep"})
old_data = old_data[["profile_name","year","rep_period","timestep","value"]]
old_data = old_data[~old_data["profile_name"].str.contains("_H_")]

old_data.to_csv("1h/profiles-rep-periods.csv", index=False)

df = pd.read_csv("1h/assets-profiles.csv")
df = df[~df["profile_name"].str in ]

# print(df)