import pandas as pd
import json

# Load the generated data
with open('data/job_listings_summary.json', 'r') as f:
    data = json.load(f)

df = pd.DataFrame(data)

print('Current data size:')
print(f'Total rows: {len(df):,}')
print(f'File size: 39 MB')
print(f'Unique subagencies: {df["Subagency"].nunique():,}')

# Analyze what happens if we aggregate without subagency
df_no_sub = df.groupby(['Department', 'Agency', 'Appointment_Type', 'Occupation_Series']).agg({
    'listings2024Value': 'sum',
    'listings2025Value': 'sum'
}).reset_index()

print(f'\nWithout subagency:')
print(f'Total rows: {len(df_no_sub):,}')
print(f'Estimated size: {len(df_no_sub) * 200 / 1024:.1f} KB')

# Check subagencies with significant data
sig_subagencies = df.groupby('Subagency').agg({
    'listings2024Value': 'sum',
    'listings2025Value': 'sum'
}).reset_index()
sig_subagencies['total'] = sig_subagencies['listings2024Value'] + sig_subagencies['listings2025Value']
sig_subagencies = sig_subagencies[sig_subagencies['total'] > 100].sort_values('total', ascending=False)

print(f'\nSubagencies with >100 total listings: {len(sig_subagencies):,}')
print(f'These account for {sig_subagencies["total"].sum():,} of {df["listings2024Value"].sum() + df["listings2025Value"].sum():,} total listings')
print(f'Percentage of total: {sig_subagencies["total"].sum() / (df["listings2024Value"].sum() + df["listings2025Value"].sum()) * 100:.1f}%')

# Show top subagencies
print('\nTop 10 subagencies by total listings:')
for _, row in sig_subagencies.head(10).iterrows():
    print(f'  {row["Subagency"]}: {int(row["total"]):,} listings')