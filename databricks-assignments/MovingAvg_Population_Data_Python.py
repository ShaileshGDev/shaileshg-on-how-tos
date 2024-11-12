# Databricks notebook source

import pandas as pd

# COMMAND ----------

# DBTITLE 1,Inserting data
data = {
    'date': ['2024-08-01', '2024-08-01', '2024-08-01',
             '2024-08-02', '2024-08-02', '2024-08-02',
             '2024-08-03', '2024-08-03', '2024-08-03',
             '2024-08-04', '2024-08-04', '2024-08-04',
             '2024-08-05', '2024-08-05', '2024-08-05',
             '2024-08-07', '2024-08-07', '2024-08-07',
             '2024-08-08', '2024-08-08', '2024-08-08'],
    'type': ['c1', 'c2', 'c3',
             'c1', 'c2', 'c3',
             'c1', 'c2', 'c3',
             'c1', 'c2', 'c3',
             'c1', 'c2', 'c3',
             'c1', 'c2', 'c3',
             'c1', 'c2', 'c3'],
    'count': [10000, 10000, 10000,
              10099, 10900, 19000,
              19000, 10700, 10030,
              7000, 10600, 10010,
              11000, 10030, 17000,
              18000, 10080, 15000,
              10300, 10000, 15000]
}



# COMMAND ----------

population_df = pd.DataFrame(data)
population_df['date'] = pd.to_datetime(population_df['date'])
population_df

# COMMAND ----------

# DBTITLE 1,Percent of Type population on current version of the table
current_date = population_df['date'].max()

current_population = population_df[population_df['date'] == current_date]
current_total = current_population['count'].sum()
current_population['percent'] = current_population['count'] / current_total * 100
current_population


# COMMAND ----------

# DBTITLE 1,Percent of Type population on previous version of the table for last three to five days
population_df = population_df.sort_values(by=['date', 'type'])

# Lag function to get the previous day's population
population_df['prev_count'] = population_df.groupby('type')['count'].shift(1)

# Calculate percentage population for the previous days
population_df['prev_total'] = population_df.groupby('date')['prev_count'].transform('sum')
population_df['prev_percent'] = population_df['prev_count'] / population_df['prev_total'] * 100

# Filter data for the last 3 to 5 days
filtered_df = population_df[(population_df['date'] >= current_date - pd.Timedelta(days=4)) & 
                            (population_df['date'] < current_date)]
filtered_df


# COMMAND ----------

# DBTITLE 1,Flag the type of devices where there is a change of percent population lets say 5% or 2%
merged_df = pd.merge(current_population[['type', 'percent']],
                     filtered_df[['date', 'type', 'prev_count', 'prev_percent']],
                     on='type', how='left')

# Calculate the percentage change
merged_df['percent_change'] = merged_df['percent'] - merged_df['prev_percent']

# Flag the types with significant changes
merged_df['flag_5_percent'] = abs(merged_df['percent_change']) > 5
merged_df['flag_2_percent'] = abs(merged_df['percent_change']) > 2

# Display all columns
result = merged_df[['date','type', 'percent', 'prev_count', 'prev_percent', 'percent_change', 'flag_5_percent', 'flag_2_percent']]
result

# COMMAND ----------


