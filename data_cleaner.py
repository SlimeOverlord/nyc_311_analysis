import pandas as pd
import dask.dataframe as dd

df = dd.read_csv("data/nyc_311_2020.csv", usecols = [1, 2, 8], header=None, assume_missing=True)
nyc_df = df.compute()

nyc_df = nyc_df.dropna(subset=[1,2,8])

nyc_df[1] = pd.to_datetime(nyc_df[1], errors='coerce')
nyc_df[2] = pd.to_datetime(nyc_df[2], errors='coerce')

nyc_df[8] = nyc_df[8].astype(int)

nyc_df = nyc_df[(nyc_df[1] <= nyc_df[2])]

nyc_df = nyc_df.rename(columns={1 : 'Start Date', 2 : 'End Date', 8: 'Zipcode'})

nyc_df['Create-to-closed Time'] = ((nyc_df['End Date'] - nyc_df['Start Date']).dt.total_seconds()/3600).round(2)
nyc_df['Month'] = nyc_df['End Date'].dt.month 

nyc_df = nyc_df[['Zipcode', 'Create-to-closed Time', 'Month']]

monthly_avg_df = nyc_df.groupby('Month')['Create-to-closed Time'].mean().reset_index()
monthly_avg_df_by_zip = nyc_df.groupby(['Zipcode', 'Month'])['Create-to-closed Time'].mean().reset_index()

monthly_avg_df.to_csv("data/monthly_avg.csv", index=False)
monthly_avg_df_by_zip.to_csv("data/monthly_avg_by_zip.csv", index=False)