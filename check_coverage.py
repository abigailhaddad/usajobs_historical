#!/usr/bin/env python3
import pandas as pd

# Load unified jobs
df = pd.read_parquet("usajobs_pipeline/data_parquet/unified_jobs/unified_jobs_20250613_161656.parquet")
print(f"Total unified jobs: {len(df)}")

# Separate historical and current
hist_mask = df['data_sources'].str.contains('historical', na=False)
curr_mask = df['data_sources'].str.contains('current', na=False)

hist_df = df[hist_mask]
curr_df = df[curr_mask]

print(f"Historical records: {len(hist_df)}")
print(f"Current records: {len(curr_df)}")

# Check key field coverage for historical
fields = ['major_duties', 'qualification_summary', 'requirements', 'education']

print("\nHistorical field coverage:")
for field in fields:
    has_content = (hist_df[field].notna()) & (hist_df[field] != "")
    pct = has_content.sum() / len(hist_df) * 100
    print(f"  {field}: {has_content.sum()}/{len(hist_df)} = {pct:.1f}%")

# Check if there's scraped content
has_scraped = (hist_df['scraped_sections'].notna()) & (hist_df['scraped_sections'] != "") & (hist_df['scraped_sections'] != "{}")
print(f"\nHistorical with scraped content: {has_scraped.sum()}/{len(hist_df)} = {has_scraped.sum()/len(hist_df)*100:.1f}%")

# Check sample scraped content
if has_scraped.sum() > 0:
    sample = hist_df[has_scraped].iloc[0]
    print(f"\nSample scraped content for {sample['control_number']}:")
    print(f"Scraped sections: {sample['scraped_sections'][:200]}...")