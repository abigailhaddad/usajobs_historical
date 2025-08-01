# USAJobs Status Tracker - Netlify Deployment

## Quick Deploy

1. Run the optimization script to create the lightweight data file:
   ```bash
   python3 create_optimized_data.py
   ```

2. Upload these files to Netlify:
   - `index.html`
   - `app.js`
   - `job_status_data_optimized.json`
   - `_headers` (for caching)

## File Sizes

- Full data: ~11 MB
- Optimized data: ~6.6 MB (42% smaller)
- Gzipped (Netlify will do this): ~0.7 MB (93% smaller)

## Performance Tips

- Netlify automatically gzips files, so the actual download will be ~0.7 MB
- The `_headers` file sets appropriate caching for better performance
- The app tries to load the optimized version first, with fallbacks

## Updating Data

To update the data on Netlify:

1. Run the update script locally:
   ```bash
   cd job_status_tracker
   source ../venv/bin/activate
   python update_active_statuses.py --year 2025
   python extract_job_status_data.py
   python create_optimized_data.py
   ```

2. Upload the new `job_status_data_optimized.json` to Netlify

## Note

The dashboard is self-contained and doesn't need any server-side processing. It's a pure static site that loads JSON data.