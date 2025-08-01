#!/bin/bash
# Prepare job_status_tracker for Netlify deployment

echo "Preparing job_status_tracker for Netlify deployment..."

# Re-run the extraction to ensure data is up to date
echo "Updating data..."
cd ..
source venv/bin/activate
cd job_status_tracker
python extract_job_status_data.py

# Check file sizes
echo -e "\nFile sizes:"
ls -lh *.json *.html *.js

# Create a netlify-ready folder
echo -e "\nCreating netlify_deploy folder..."
mkdir -p netlify_deploy
cp index.html netlify_deploy/
cp app.js netlify_deploy/
cp job_status_data.min.json netlify_deploy/

echo -e "\nDeployment folder ready at: netlify_deploy/"
echo "Upload the contents of netlify_deploy/ to Netlify"

# Show final size
echo -e "\nTotal size of deployment:"
du -sh netlify_deploy/