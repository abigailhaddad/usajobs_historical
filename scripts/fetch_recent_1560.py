#!/usr/bin/env python3
"""
Fetch current 1560 (Data Science) series jobs from USAJobs API
and write a JSON file with full details including duties and qualifications.

Usage:
    python scripts/fetch_recent_1560.py
    python scripts/fetch_recent_1560.py --output recent_jobs.json
"""

import argparse
import json
import os
import re
import time
from html import unescape

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

BASE_URL = "https://data.usajobs.gov/api/Search"
SERIES_CODE = "1560"


def get_headers():
    api_key = os.getenv("USAJOBS_API_TOKEN") or os.getenv("USAJOBS_TOKEN")
    if not api_key:
        raise ValueError("Set USAJOBS_API_TOKEN environment variable")
    return {
        "Host": "data.usajobs.gov",
        "Authorization-Key": api_key,
    }


def clean_text(text):
    if not text:
        return None
    if isinstance(text, list):
        text = " ".join(str(item) for item in text)
    text = re.sub(r"<[^>]+>", " ", str(text))
    text = unescape(text)
    text = " ".join(text.split())
    return text or None


def extract_job(item):
    """Pull the fields we care about from a SearchResultItem."""
    job = item.get("MatchedObjectDescriptor", {})
    user_area = job.get("UserArea", {}).get("Details", {})
    remuneration = job.get("PositionRemuneration", [{}])
    grades = job.get("JobGrade", [])

    # Build location string
    locations = job.get("PositionLocation", [])
    location_parts = []
    for loc in locations:
        city = loc.get("CityName", "")
        state = loc.get("CountrySubDivisionCode", "")
        if city:
            location_parts.append(f"{city}, {state}" if state else city)

    position_uri = job.get("PositionURI", "")

    return {
        "control_number": job.get("PositionID"),
        "title": job.get("PositionTitle"),
        "organization": job.get("OrganizationName"),
        "department": job.get("DepartmentName"),
        "location": "; ".join(location_parts) if location_parts else None,
        "pay_scale": grades[0].get("Code") if grades else None,
        "low_grade": user_area.get("LowGrade"),
        "high_grade": user_area.get("HighGrade"),
        "min_salary": remuneration[0].get("MinimumRange") if remuneration else None,
        "max_salary": remuneration[0].get("MaximumRange") if remuneration else None,
        "open_date": job.get("PositionStartDate"),
        "close_date": job.get("PositionEndDate"),
        "url": position_uri,
        "telework": user_area.get("TeleworkEligible"),
        "appointment_type": job.get("PositionOfferingType", [{}])[0].get("Name", "")
            if job.get("PositionOfferingType") else None,
        "duties": clean_text(user_area.get("MajorDuties")),
        "qualifications": clean_text(job.get("QualificationSummary")),
    }


def fetch_all_1560_jobs(headers):
    """Fetch all current 1560-series jobs, paginating as needed."""
    jobs = []
    page = 1
    while True:
        params = {
            "JobCategoryCode": SERIES_CODE,
            "ResultsPerPage": 500,
            "Fields": "full",
            "Page": page,
        }
        print(f"Fetching page {page}...")
        resp = requests.get(BASE_URL, headers=headers, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("SearchResult", {}).get("SearchResultItems", [])
        if not items:
            break

        total = data["SearchResult"].get("SearchResultCountAll", 0)
        for item in items:
            jobs.append(extract_job(item))

        print(f"  Got {len(items)} jobs (total so far: {len(jobs)}/{total})")
        if len(jobs) >= total:
            break
        page += 1
        time.sleep(0.5)

    return jobs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="recent_jobs.json")
    args = parser.parse_args()

    headers = get_headers()
    jobs = fetch_all_1560_jobs(headers)
    print(f"Fetched {len(jobs)} total 1560-series jobs")

    with open(args.output, "w") as f:
        json.dump(jobs, f, indent=2)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
