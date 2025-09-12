# Deployment Strategy for USAJobs Historic

This repository manages 3+ Netlify sites and uses GitHub Actions for data updates. Here's the branching and deployment strategy:

## Branches

### `main` (Production)
- **Protected branch** - requires PR reviews
- Deploys to all production Netlify sites
- GitHub Actions run here for scheduled data updates
- Only merge here after testing in staging

### `staging`
- Test changes before production
- Deploys to staging versions of sites (e.g., `staging--your-site.netlify.app`)
- Run test scripts here before merging to main

### `dev-tracking`
- Development branch for tracking site changes
- Only deploys tracking site (other sites ignore)

### `dev-questionnaires`
- Development branch for questionnaire site changes  
- Only deploys questionnaire site (other sites ignore)

### `dev-nps`
- Development branch for National Park Service analysis
- Only deploys NPS site (other sites ignore)

## Netlify Configuration

Each site needs to be configured in Netlify dashboard:

### Production Deployments
- **Production branch**: `main`
- **Deploy previews**: Enabled for PRs

### Branch Deployments
1. **Tracking Site**
   - Also deploy from: `staging`, `dev-tracking`
   
2. **Questionnaires Site**
   - Also deploy from: `staging`, `dev-questionnaires`
   
3. **NPS Analysis Site**
   - Also deploy from: `staging`, `dev-nps`

## GitHub Actions Configuration

### Current Setup
- Actions run on `main` branch
- Scheduled updates push directly to `main`

### Implemented Changes
1. **data-updates** branch for daily API updates
2. **questionnaire-updates** branch for questionnaire analysis  
3. **tracking-updates** branch for weekly tracking summaries
4. Actions create PRs from these branches to `main`
5. Auto-merge if tests pass, otherwise create issue

## Workflow

### For Feature Development
```bash
# Create feature branch from main
git checkout main
git pull
git checkout -b feature/your-feature

# Make changes
# Run appropriate test script
python tracking/test_tracking_artifacts.py
# or
python questionnaires/test_questionnaire_artifacts.py

# Push and create PR to staging
git push origin feature/your-feature
# Create PR to staging branch

# After staging tests pass, PR to main
```

### For Site-Specific Changes
```bash
# For tracking-only changes
git checkout dev-tracking
git pull origin main  # Keep in sync
# Make changes
git push

# For questionnaire-only changes  
git checkout dev-questionnaires
git pull origin main
# Make changes
git push
```

### For Data Updates (Automated)
1. GitHub Actions run on schedule:
   - **Daily at 2 AM EST**: API data update → `data-updates` branch
   - **Daily at 4 AM EST**: Questionnaire analysis → `questionnaire-updates` branch
   - **Weekly Mondays**: Tracking summary → `tracking-updates` branch
2. Each workflow:
   - Creates/resets its specific branch from main
   - Runs update scripts
   - Runs appropriate test scripts
   - Creates PR if changes exist
   - Auto-merges if tests pass
   - Creates issue if tests fail

## Implementation Steps

1. **Create branches**
   ```bash
   git checkout -b staging
   git push origin staging
   
   git checkout -b dev-tracking
   git push origin dev-tracking
   
   git checkout -b dev-questionnaires
   git push origin dev-questionnaires
   
   git checkout -b dev-nps
   git push origin dev-nps
   ```

2. **Update Netlify settings** (in dashboard for each site):
   - Go to Site settings > Build & deploy > Branches
   - Add branch deploy contexts

3. **GitHub Actions Updated** ✅
   - Modified all workflows to use branching strategy
   - Added test runs before merging
   - Auto-PR creation with peter-evans/create-pull-request
   - Auto-merge on test success

4. **Protect main branch**:
   - Settings > Branches > Add rule
   - Require PR reviews
   - Require status checks (tests)
   - Include administrators

## Benefits

1. **Isolation**: Changes to one site don't trigger rebuilds of others
2. **Testing**: All changes tested in staging before production  
3. **Safety**: Protected main branch prevents accidental breaks
4. **Automation**: Data updates can run without breaking sites
5. **Flexibility**: Can develop and test each site independently

## Emergency Procedures

If automated updates break something:
1. Revert the merge on `main`
2. Fix the issue in `data-updates` branch
3. Re-run tests
4. Create new PR when fixed

## Notes

- The existing `netlify.toml` build ignore rules still apply
- This strategy works with the current monorepo structure
- Can be adjusted as the project grows