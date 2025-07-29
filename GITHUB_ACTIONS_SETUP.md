# GitHub Actions Setup Guide

This guide helps you deploy the USAJobs data pipeline to GitHub Actions.

## ⚠️ Important: Git LFS Required

This repository uses Git Large File Storage (LFS) for parquet data files. The GitHub Actions workflow is configured to handle this automatically.

## 🚀 Quick Setup

### 1. Add Your USAJobs API Token

1. Go to your repository on GitHub
2. Click **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Add:
   - Name: `USAJOBS_API_TOKEN`
   - Value: Your USAJobs API token from https://developer.usajobs.gov/

### 2. Test the Workflow

After pushing this branch:

1. Go to **Actions** tab in your GitHub repository
2. Click on "Daily Data Pipeline" workflow
3. Click **Run workflow** → **Run workflow** (manual trigger)
4. Watch the logs to ensure everything works

### 3. Merge to Main

Once tested successfully:

```bash
git add .
git commit -m "Add GitHub Actions for daily data pipeline"
git push origin setup-github-actions
```

Then create a Pull Request and merge to main.

## 📅 Schedule

The workflows run automatically:
- **Data Update**: Daily at 2 AM EST (7 AM UTC)
- **Questionnaire Analysis**: Daily at 4 AM EST (9 AM UTC)
- **Duration**: ~40-60 minutes for data update, ~30+ minutes for questionnaires

Two separate workflows ensure the questionnaire analysis always works with the latest data.

## 🔍 Monitoring

### Check Run Status
- Go to **Actions** tab to see all workflow runs
- Green checkmark = success
- Red X = failure (creates an issue automatically)

### View Logs
- Click on any workflow run to see detailed logs
- Each job shows real-time output

### Download Artifacts
- Questionnaire analysis HTML is saved as an artifact
- Download from the workflow run page

## 🛠️ Troubleshooting

### If the workflow fails:

1. **Check the logs** in the Actions tab
2. **Common issues**:
   - API token not set correctly
   - USAJobs API is down
   - Git push conflicts (rare with this setup)
   - Playwright/Quarto installation issues

### Manual intervention:

If needed, you can:
1. Run the workflow manually from Actions tab
2. Run locally and push changes
3. Disable the schedule temporarily by commenting out the `schedule:` section

## 💰 Cost

- **Public repositories**: FREE unlimited Actions minutes
- **Private repositories**: 2,000 free minutes/month, then $0.008/minute

Your daily runs use approximately:
- 60 minutes × 30 days = 1,800 minutes/month

## 🔧 Customization

### Change Schedule

Edit `.github/workflows/daily-pipeline.yml`:

```yaml
schedule:
  # Examples:
  - cron: '0 7 * * *'    # Daily at 7 AM UTC
  - cron: '0 7 * * 1'    # Weekly on Mondays
  - cron: '0 7 1 * *'    # Monthly on the 1st
```

Use https://crontab.guru/ to create cron expressions.

### Disable Questionnaire Analysis

To run only data updates, comment out the `analyze-questionnaires` job.

### Add Notifications

You can add Slack/email notifications by adding steps to the workflow.