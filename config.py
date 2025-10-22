import os
from datetime import datetime, timedelta

# App Configuration
APP_ID = 'in.swiggy.android'
LANG = 'en'
COUNTRY = 'in'

# Batch Processing Configuration
START_DATE = datetime(2025, 6, 1).date()
DAILY_BATCH_SIZE = 200

# API Configuration
GROQ_API_KEY = os.getenv('GROQ_API_KEY', 'YOUR_GROQ_API_KEY_HERE')
GROQ_MODEL = 'llama3-70b-8192'  # Fast and accurate

# Path Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
DB_DIR = os.path.join(DATA_DIR, 'database')
BATCH_STATUS_DIR = os.path.join(DATA_DIR, 'batch_status')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
DB_PATH = os.path.join(DB_DIR, 'reviews.db')

# Create directories
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)
os.makedirs(BATCH_STATUS_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Topic Extraction Settings
REVIEWS_PER_API_CALL = 10
API_DELAY_SECONDS = 2
SIMILARITY_THRESHOLD = 0.85

# Report Settings
TREND_WINDOW_DAYS = 30