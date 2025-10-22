#!/usr/bin/env python3
import logging
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'full_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)

logger = logging.getLogger(__name__)

def main():
    print("╔═══════════════════════════════════════════════════════════════╗")
    print("║     SENIOR AI ENGINEER ASSIGNMENT - PULSEGEN TECHNOLOGIES    ║")
    print("║              Complete AI Agentic Pipeline                     ║")
    print("╚═══════════════════════════════════════════════════════════════╝\n")
    
    start_time = datetime.now()
    
    # Phase 1: Data Collection (Already Done)
    print("📋 PHASE 1: Review Collection")
    print("   Status: ✅ COMPLETED (10K reviews, 60 days)")
    print("   Next: Run Phase 2 for topic extraction\n")
    
    response = input("⚠️  Phase 2 will make ~600 API calls to Groq (~20 mins). Continue? (y/n): ")
    if response.lower() != 'y':
        print("Aborted.")
        return
    
    # Phase 2: AI Topic Extraction
    try:
        print("\n" + "="*60)
        print("🤖 PHASE 2: AI Topic Extraction (Agentic)")
        print("="*60)
        
        from src.main_phase2 import run_phase2
        phase2_result = run_phase2()
        
        if phase2_result is None:
            print("❌ Phase 2 failed. Aborting pipeline.")
            return
        
    except Exception as e:
        logger.error(f"Phase 2 failed: {e}")
        print(f"\n❌ Phase 2 Error: {e}")
        return
    
    # Phase 3: Trend Analysis
    try:
        print("\n" + "="*60)
        print("📊 PHASE 3: Trend Analysis & Report")
        print("="*60)
        
        from src.main_phase3 import run_phase3
        phase3_result = run_phase3()
        
        if phase3_result is None:
            print("❌ Phase 3 failed.")
            return
        
    except Exception as e:
        logger.error(f"Phase 3 failed: {e}")
        print(f"\n❌ Phase 3 Error: {e}")
        return
    
    # Final Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "╔" + "="*78 + "╗")
    print("║" + " "*20 + "🎉 PIPELINE COMPLETED SUCCESSFULLY!" + " "*23 + "║")
    print("╚" + "="*78 + "╝")
    print(f"\n⏱️  Total Duration: {duration}")
    print(f"📊 Phase 2: {phase2_result['batches_processed']} batches, {phase2_result['total_topics']} topics")
    print(f"📈 Phase 3: {phase3_result['total_topics']} unique topics")
    print(f"📁 Output Files:")
    print(f"   - {phase3_result['report_file']}")
    print(f"   - {phase3_result['summary_file']}")
    print("\n✅ All deliverables ready for submission!")
    print("="*80)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
    except Exception as e:
        logger.exception("Pipeline failed")
        print(f"\n❌ Pipeline failed: {e}")