import logging
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

sys.path.append(os.path.dirname(__file__))
from config import DB_PATH, OUTPUT_DIR, TREND_WINDOW_DAYS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('phase3_trend_analysis.log')
    ]
)

logger = logging.getLogger(__name__)

class TrendAnalyzer:
    def __init__(self):
        self.db_path = DB_PATH
        self.output_dir = Path(OUTPUT_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_trend_report(self, target_date: datetime.date = None, window_days: int = TREND_WINDOW_DAYS):
        logger.info(f"📊 Generating trend report for last {window_days} days")
        
        if target_date is None:
            target_date = datetime.now().date()
        
        start_date = target_date - timedelta(days=window_days - 1)
        
        logger.info(f"Date range: {start_date} to {target_date}")
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            topic_name,
            date,
            COUNT(*) as frequency
        FROM processed_topics
        WHERE date BETWEEN ? AND ?
        GROUP BY topic_name, date
        ORDER BY topic_name, date
        """
        
        df = pd.read_sql_query(query, conn, params=[start_date.strftime('%Y-%m-%d'), target_date.strftime('%Y-%m-%d')])
        conn.close()
        
        if df.empty:
            logger.warning("No topic data found for the specified date range")
            return None
        
        logger.info(f"Retrieved {len(df)} topic-date records")
        
        pivot_table = df.pivot_table(
            index='topic_name',
            columns='date',
            values='frequency',
            fill_value=0,
            aggfunc='sum'
        )
        
        pivot_table.columns = pd.to_datetime(pivot_table.columns).strftime('%Y-%m-%d')
        
        pivot_table = pivot_table.sort_values(by=pivot_table.columns[-1], ascending=False)
        
        output_file = self.output_dir / f'trend_report_{target_date}.csv'
        pivot_table.to_csv(output_file)
        logger.info(f"✅ Trend report saved: {output_file}")
        
        summary = self._generate_summary(pivot_table, start_date, target_date)
        summary_file = self.output_dir / f'trend_summary_{target_date}.txt'
        with open(summary_file, 'w') as f:
            f.write(summary)
        logger.info(f"✅ Summary saved: {summary_file}")
        
        return {
            'report_file': str(output_file),
            'summary_file': str(summary_file),
            'total_topics': len(pivot_table),
            'date_range': {'start': start_date, 'end': target_date}
        }
    
    def _generate_summary(self, pivot_table: pd.DataFrame, start_date, end_date) -> str:
        total_topics = len(pivot_table)
        total_mentions = pivot_table.sum().sum()
        
        top_10 = pivot_table.sum(axis=1).nlargest(10)
        
        recent_col = pivot_table.columns[-1]
        trending_topics = pivot_table.nlargest(10, recent_col)
        
        summary = f"""
╔═══════════════════════════════════════════════════════════════╗
║              TREND ANALYSIS REPORT SUMMARY                    ║
╚═══════════════════════════════════════════════════════════════╝

📅 Date Range: {start_date} to {end_date}
📊 Total Topics: {total_topics}
📈 Total Mentions: {int(total_mentions)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔥 TOP 10 TOPICS (Overall Frequency):
"""
        for i, (topic, count) in enumerate(top_10.items(), 1):
            summary += f"\n{i:2d}. {topic:40s} {int(count):4d} mentions"
        
        summary += f"""

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 TRENDING NOW (Most Recent Day: {recent_col}):
"""
        for i, (topic, row) in enumerate(trending_topics.iterrows(), 1):
            recent_count = int(row[recent_col])
            summary += f"\n{i:2d}. {topic:40s} {recent_count:4d} mentions"
        
        summary += "\n\n" + "═" * 65 + "\n"
        
        return summary
    
    def get_topic_stats(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(DISTINCT topic_name) FROM processed_topics")
        unique_topics = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM processed_topics")
        total_mentions = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT topic_name, COUNT(*) as count 
            FROM processed_topics 
            GROUP BY topic_name 
            ORDER BY count DESC 
            LIMIT 10
        """)
        top_topics = cursor.fetchall()
        
        conn.close()
        
        return {
            'unique_topics': unique_topics,
            'total_mentions': total_mentions,
            'top_topics': top_topics
        }

def run_phase3():
    print("🚀 Phase 3: Trend Analysis & Report Generation")
    print("=" * 60)
    
    try:
        analyzer = TrendAnalyzer()
        
        stats = analyzer.get_topic_stats()
        print(f"\n📊 Topic Statistics:")
        print(f"   Unique Topics: {stats['unique_topics']}")
        print(f"   Total Mentions: {stats['total_mentions']}")
        
        result = analyzer.generate_trend_report()
        
        if result:
            print(f"\n{'='*80}")
            print("🎉 PHASE 3 COMPLETED!")
            print(f"{'='*80}")
            print(f"📊 Total Topics: {result['total_topics']}")
            print(f"📅 Range: {result['date_range']['start']} to {result['date_range']['end']}")
            print(f"📁 Report: {result['report_file']}")
            print(f"📄 Summary: {result['summary_file']}")
            print(f"{'='*80}")
            
            return result
        else:
            print("❌ No data available for trend report")
            return None
        
    except Exception as e:
        logger.error(f"❌ Phase 3 failed: {e}")
        raise

if __name__ == "__main__":
    run_phase3()