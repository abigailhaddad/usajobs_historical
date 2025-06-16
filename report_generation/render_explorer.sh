#!/bin/bash

# Render the USAJobs Explorer dashboard

echo "🔄 Rendering USAJobs Explorer Dashboard..."
echo "==============================================="

# Check if data file exists
if [ ! -f "../data/usajobs.parquet" ]; then
    echo "❌ Data file not found: ../data/usajobs.parquet"
    echo "   Run the pipeline first: python run_pipeline.py"
    exit 1
fi

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python is not installed. Please install Python to render the dashboard."
    exit 1
fi

# Check if required Python packages are available
python3 -c "import pandas, plotly" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "⚠️  Required Python packages missing. Installing..."
    pip install pandas plotly
fi

# Check if Quarto is installed
if ! command -v quarto &> /dev/null; then
    echo "❌ Quarto is not installed. Please install Quarto to render the dashboard."
    echo "   Visit: https://quarto.org/docs/get-started/"
    exit 1
else
    echo "📊 Using Quarto to render dashboard..."
    quarto render job_explorer.qmd --output-dir ../reports
fi

# Check if output was created
if [ -f "../reports/job_explorer.html" ]; then
    echo ""
    echo "✅ Dashboard rendered successfully!"
    echo "📊 Open ../reports/job_explorer.html in your browser"
    echo ""
    echo "Features:"
    echo "  • Interactive monthly hiring trends"
    echo "  • Agency and occupation patterns"
    echo "  • Who is hiring what when"
    
    # Try to open in browser (macOS)
    if command -v open &> /dev/null; then
        echo ""
        echo "🌐 Opening dashboard in browser..."
        open ../reports/job_explorer.html
    fi
else
    echo "❌ Dashboard rendering failed"
    echo "   Check error messages above"
    exit 1
fi