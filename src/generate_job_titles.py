#!/usr/bin/env python3
"""
Generate Job Titles using LLM
Analyzes USAJobs data fields to generate plain language job titles
"""

import json
import os
from dotenv import load_dotenv
from litellm import completion
from pydantic import BaseModel, Field
from typing import Optional, List

# Load environment variables
load_dotenv()

class PlainLanguageAnalysis(BaseModel):
    """Analysis of whether a job title follows plain language principles"""
    isPlainLanguage: bool = Field(
        description="Whether the title follows plain language principles"
    )
    reasons: List[str] = Field(
        description="2-4 specific reasons explaining why or why not"
    )
    suggestion: Optional[str] = Field(
        description="If not plain language, a suggested alternative title",
        default=None
    )

def analyze_job_title(job_data, model="gpt-4o-mini"):
    """
    Analyze job title using plain language principles
    """
    current_title = job_data.get('PositionTitle', 'Unknown')
    job_summary = job_data.get('JobSummary', '')
    qualification_summary = job_data.get('QualificationSummary', '')
    major_duties = job_data.get('MajorDuties', [])
    
    # Convert major duties list to string
    duties_text = '\n'.join(major_duties[:3]) if isinstance(major_duties, list) else str(major_duties)[:1000]
    
    # Combine context for better suggestions
    context = f"""
Job Summary: {job_summary[:500]}...

Key Qualifications: {qualification_summary[:500]}...

Main Duties: {duties_text[:500]}...
"""

    system_prompt = """You are an expert in plain language writing for federal government job titles. Your task is to analyze whether a given job title follows plain language principles.

Plain language job titles should:
- Be clear and understandable to the general public
- Be compelling and engaging to a potential employee
- Avoid jargon, acronyms, or technical terms unless describing a specialized role where you need to know jargon to do the job or it is a leadership role of a specific office
- Clearly indicate what the person does
- Use common, everyday words when possible
- Be concise but descriptive
- Technical roles should be written in a way that describes the function of the role, so generic IT Specialist does not work, but software engineer, network engineer, etc. do

IMPORTANT: One-word trade titles are often excellent plain language titles. Examples include:
- Electrician
- Plumber
- Pipefitter
- Carpenter
- Welder
- Mechanic
- Painter
These should generally be kept as-is unless they have unnecessary additions or are in ALL CAPS

Respond with a JSON object containing:
- "isPlainLanguage": boolean indicating if the title follows plain language principles
- "reasons": array of strings explaining why or why not (provide 2-4 specific reasons)
- "suggestion": if not plain language, provide ONE suggested alternative title based on the job context (never use "or" to provide multiple options - pick the single best title)"""

    user_prompt = f"""Analyze this federal government job title: "{current_title}"

Additional context about the role:
{context}"""

    try:
        response = completion(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format=PlainLanguageAnalysis,
            temperature=0.3
        )
        
        # Parse the response
        if hasattr(response.choices[0].message, 'content'):
            content = response.choices[0].message.content
            if isinstance(content, str):
                return json.loads(content)
            return content
        else:
            return response.choices[0].message.model_dump()
            
    except Exception as e:
        print(f"Error analyzing title: {e}")
        return None

def generate_title(job_data, model="gpt-4o-mini"):
    """
    Generate plain language title if needed
    """
    analysis = analyze_job_title(job_data, model)
    
    if not analysis:
        return None
    
    # If title is already plain language, keep it
    if analysis.get('isPlainLanguage', False):
        reasons = analysis.get('reasons', [])
        print(f"  ✓ Plain language title: {reasons[0] if reasons else 'Good as-is'}")
        return job_data.get('PositionTitle', 'Unknown')
    
    # Otherwise, use the suggestion
    reasons = analysis.get('reasons', [])
    print(f"  → Needs improvement: {reasons[0] if reasons else 'Not plain language'}")
    
    suggestion = analysis.get('suggestion')
    if suggestion:
        return suggestion
    else:
        # Fallback to original if no suggestion
        return job_data.get('PositionTitle', 'Unknown')


def process_jobs_file(input_file, output_file=None):
    """
    Process USAJobs JSON file and generate titles for all jobs
    
    Args:
        input_file: Path to USAJobs JSON file
        output_file: Path to save results (optional)
    """
    # Load jobs data
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    # Extract job items
    jobs = data.get('SearchResult', {}).get('SearchResultItems', [])
    
    results = []
    
    for i, job in enumerate(jobs):
        # Get job details
        descriptor = job.get('MatchedObjectDescriptor', {})
        user_area = descriptor.get('UserArea', {})
        details = user_area.get('Details', {})
        
        # Extract original title
        original_title = descriptor.get('PositionTitle', 'Unknown')
        position_id = descriptor.get('PositionID', 'N/A')
        
        print(f"\nProcessing job {i+1}/{len(jobs)}: {original_title} (ID: {position_id})")
        
        # Add position title to details for the prompt
        details['PositionTitle'] = original_title
        
        # Generate new title
        generated_title = generate_title(details)
        
        if generated_title:
            print(f"Result: {generated_title}")
            
            results.append({
                'position_id': position_id,
                'original_title': original_title,
                'generated_title': generated_title,
                'job_summary': details.get('JobSummary', '')[:200] + '...',  # First 200 chars
                'organization': descriptor.get('OrganizationName', '')
            })
        else:
            print("Failed to generate title")
    
    # Save results if output file specified
    if output_file:
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved to {output_file}")
    
    return results


def main():
    """Main function to demonstrate usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate plain language job titles from USAJobs data")
    parser.add_argument("input_file", help="Path to USAJobs JSON file")
    parser.add_argument("--output", help="Output file for results", default="generated_titles.json")
    parser.add_argument("--model", help="LLM model to use", default="gpt-4o-mini")
    parser.add_argument("--sample", type=int, help="Process only first N jobs", default=None)
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found")
        return
    
    # If sample specified, limit the data
    if args.sample:
        with open(args.input_file, 'r') as f:
            data = json.load(f)
        
        # Limit the number of jobs
        jobs = data.get('SearchResult', {}).get('SearchResultItems', [])
        data['SearchResult']['SearchResultItems'] = jobs[:args.sample]
        
        # Save to temp file
        temp_file = 'temp_sample.json'
        with open(temp_file, 'w') as f:
            json.dump(data, f)
        
        # Process temp file
        results = process_jobs_file(temp_file, args.output)
        
        # Clean up
        os.remove(temp_file)
    else:
        results = process_jobs_file(args.input_file, args.output)
    
    print(f"\nProcessed {len(results)} jobs")


if __name__ == "__main__":
    main()