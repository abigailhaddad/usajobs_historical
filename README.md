# USAJobs Data Pipeline

Fetches new job listings from USAJobs API and generates improved job titles using AI.

## Setup

1. **Create virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Create .env file:**
   ```bash
   # .env
   USAJOBS_API_TOKEN=your_usajobs_api_key_here
   DATABASE_URL=your_postgresql_connection_string_here
   OPENAI_API_KEY=your_openai_api_key_here
   ```

## Run Pipeline

**Test with small sample first:**
```bash
# Process just 10 jobs (good for testing)
./run_full_pipeline.sh sample 10

# Process 100 jobs
./run_full_pipeline.sh sample 100
```

**Process new jobs by date:**
```bash
# Process jobs from last 1 day
./run_full_pipeline.sh days 1

# Process jobs from last 3 days  
./run_full_pipeline.sh days 3

# Process jobs from last week
./run_full_pipeline.sh days 7
```

**⚠️ Important:** This will push all processed results to your PostgreSQL database.

The pipeline will:
1. Fetch jobs from USAJobs API
2. Generate better job titles using AI
3. Save results to `data/` folder AND your database