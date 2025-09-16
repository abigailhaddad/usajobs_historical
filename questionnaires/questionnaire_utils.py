"""Shared utilities for questionnaire processing"""
import re
from pathlib import Path


# Common paths
RAW_QUESTIONNAIRES_DIR = Path('./raw_questionnaires')
QUESTIONNAIRE_LINKS_CSV = Path('./questionnaire_links.csv')


def transform_monster_url(url):
    """Transform Monster dashboard URL to preview format"""
    if 'monstergovt.com' in url and '/ros/rosDashboard.hms' in url:
        # Handle both /ros/rosDashboard and /nga/ros/rosDashboard patterns
        match = re.search(r'https://jobs\.monstergovt\.com/([^/]+)/(?:nga/)?ros/rosDashboard\.hms\?O=(\d+)&J=(\d+)', url)
        if match:
            subdomain = match.group(1)
            org_id = match.group(2)
            job_num = match.group(3)
            return f'https://jobs.monstergovt.com/{subdomain}/vacancy/previewVacancyQuestions.hms?orgId={org_id}&jnum={job_num}'
    elif 'monstergovt.com' in url and '/rospost/' in url:
        match = re.search(r'https://jobs\.monstergovt\.com/([^/]+)/rospost/\?O=(\d+)&J=(\d+)', url)
        if match:
            subdomain = match.group(1)
            org_id = match.group(2)
            job_num = match.group(3)
            return f'https://jobs.monstergovt.com/{subdomain}/vacancy/previewVacancyQuestions.hms?orgId={org_id}&jnum={job_num}'
    return url


def extract_questionnaire_id(url):
    """Extract questionnaire ID and prefix from URL"""
    if 'usastaffing.gov' in url:
        match = re.search(r'ViewQuestionnaire/(\d+)', url)
        file_id = match.group(1) if match else 'unknown'
        return 'usastaffing', file_id
    elif 'monstergovt.com' in url:
        # Try jnum first
        match = re.search(r'jnum=(\d+)', url)
        if not match:
            # Try J= format
            match = re.search(r'J=(\d+)', url)
        file_id = match.group(1) if match else str(hash(url))[:8]
        return 'monster', file_id
    else:
        file_id = str(hash(url))[:8]
        return 'other', file_id


def get_questionnaire_filename(url):
    """Get the filename for a questionnaire based on URL"""
    prefix, file_id = extract_questionnaire_id(url)
    return f'{prefix}_{file_id}.txt'


def get_questionnaire_filepath(url):
    """Get the full file path for a questionnaire"""
    return RAW_QUESTIONNAIRES_DIR / get_questionnaire_filename(url)


def questionnaire_exists(url):
    """Check if a questionnaire has already been scraped"""
    return get_questionnaire_filepath(url).exists()


def create_git_commit_message(new_count, scraped_count, failed_count, total_links, total_files):
    """Create standardized git commit message for questionnaire updates"""
    if new_count > 0:
        message = f"""Update questionnaires: {new_count:,} new links found, {scraped_count:,} scraped

- Extracted {new_count:,} new questionnaire links
- Scraped {scraped_count:,} questionnaire files  
- Failed to scrape: {failed_count} files
- Total questionnaire links: {total_links:,}
- Total scraped files: {total_files:,}"""
    else:
        message = f"""Update questionnaires: scraped {scraped_count} previously unscraped files

- No new questionnaire links found
- Scraped {scraped_count} previously unscraped questionnaires
- Failed to scrape: {failed_count} files
- Total questionnaire links: {total_links:,}
- Total scraped files: {total_files:,}"""
    
    # Add attribution footer
    message += """

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"""
    
    return message