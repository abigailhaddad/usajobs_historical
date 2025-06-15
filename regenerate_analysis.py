#!/usr/bin/env python3
"""
Regenerate mismatch analysis after re-scraping overlap jobs
"""

import sys
sys.path.append('src')

from generate_mismatch_analysis import generate_mismatch_html
import subprocess
import os

def regenerate_analysis():
    """Regenerate both mismatch analysis and QMD report"""
    
    print("🔄 Regenerating content mismatch analysis...")
    
    try:
        # Generate the updated mismatch analysis HTML
        output_file = generate_mismatch_html()
        print(f"✅ Generated: {output_file}")
        
        # Try to regenerate the QMD report as well
        print("\n📊 Attempting to regenerate QMD analysis report...")
        
        if os.path.exists("rationalization_analysis.qmd"):
            try:
                result = subprocess.run(
                    ["quarto", "render", "rationalization_analysis.qmd"], 
                    capture_output=True, 
                    text=True,
                    timeout=300  # 5 minute timeout
                )
                
                if result.returncode == 0:
                    print("✅ Successfully regenerated rationalization_analysis.html")
                else:
                    print(f"⚠️ QMD render failed: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                print("⏰ QMD render timed out after 5 minutes")
            except FileNotFoundError:
                print("⚠️ Quarto not found - skipping QMD report regeneration")
            except Exception as e:
                print(f"⚠️ Error rendering QMD: {e}")
        else:
            print("⚠️ rationalization_analysis.qmd not found")
        
        print("\n✅ Analysis regeneration complete!")
        print(f"\n📊 View results:")
        print(f"   🔍 Content mismatches: file://{os.path.abspath('content_mismatch_analysis.html')}")
        if os.path.exists("rationalization_analysis.html"):
            print(f"   📈 Full analysis: file://{os.path.abspath('rationalization_analysis.html')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error regenerating analysis: {e}")
        return False

if __name__ == "__main__":
    regenerate_analysis()