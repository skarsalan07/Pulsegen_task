import logging
import pandas as pd
from typing import List, Dict, Any
import re
import json
from datetime import datetime
import time
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from config import REVIEWS_PER_API_CALL, API_DELAY_SECONDS
from .llm_client import LLMClient

logger = logging.getLogger(__name__)

class TopicExtractionAgent:
    def __init__(self, llm_client: LLMClient):
        self.llm = llm_client
        
        self.seed_topics = [
            "Delivery issue",
            "Food quality issue", 
            "Delivery partner behavior",
            "App technical issue",
            "Feature request",
            "Service timing request"
        ]
        
        logger.info("✅ Topic Extraction Agent initialized")
    
    def extract_topics_from_batch(self, reviews_df: pd.DataFrame, batch_date: str) -> List[Dict[str, Any]]:
        if reviews_df.empty:
            return []
        
        logger.info(f"📊 Extracting topics from {len(reviews_df)} reviews for {batch_date}")
        
        all_topics = []
        chunk_size = REVIEWS_PER_API_CALL
        
        for i in range(0, len(reviews_df), chunk_size):
            chunk = reviews_df.iloc[i:i + chunk_size]
            logger.info(f"  Processing chunk {i//chunk_size + 1}/{(len(reviews_df)-1)//chunk_size + 1}")
            
            chunk_topics = self._process_reviews_chunk(chunk, batch_date)
            all_topics.extend(chunk_topics)
            
            time.sleep(API_DELAY_SECONDS)
        
        logger.info(f"✅ Extracted {len(all_topics)} topic mentions from batch {batch_date}")
        return all_topics
    
    def _process_reviews_chunk(self, reviews_chunk: pd.DataFrame, batch_date: str) -> List[Dict[str, Any]]:
        try:
            reviews_text = self._prepare_reviews_for_llm(reviews_chunk)
            prompt = self._create_topic_extraction_prompt(reviews_text)
            llm_response = self.llm.generate(prompt)
            extracted_topics = self._parse_llm_response(llm_response, reviews_chunk, batch_date)
            return extracted_topics
            
        except Exception as e:
            logger.error(f"❌ Error processing reviews chunk: {e}")
            return []
    
    def _prepare_reviews_for_llm(self, reviews_chunk: pd.DataFrame) -> str:
        reviews_list = []
        for idx, row in reviews_chunk.iterrows():
            review_id = row.get('review_id', f'review_{idx}')
            content = row['content']
            score = row['score']
            reviews_list.append({
                'id': review_id,
                'text': content,
                'rating': int(score)
            })
        
        return json.dumps(reviews_list, ensure_ascii=False)
    
    def _create_topic_extraction_prompt(self, reviews_json: str) -> str:
        prompt = f"""Analyze these app reviews and extract specific topics/issues/requests mentioned.

SEED TOPICS (use as reference, but identify new ones too):
{', '.join(self.seed_topics)}

REVIEWS (JSON):
{reviews_json}

INSTRUCTIONS:
1. For each review, identify ALL topics mentioned
2. Consolidate similar phrases (e.g., "late delivery", "delayed order" → "Delivery issue")
3. Categorize as: issue, request, or feedback
4. Create clear, concise topic names (max 5 words)
5. Map each topic to review IDs where it appears

OUTPUT (Valid JSON only):
{{
  "topics": [
    {{
      "topic_name": "clear topic name",
      "category": "issue|request|feedback",
      "review_ids": ["review_id1", "review_id2"],
      "is_new": true
    }}
  ]
}}

Respond with ONLY the JSON, no other text."""
        
        return prompt
    
    def _parse_llm_response(self, llm_response: str, reviews_chunk: pd.DataFrame, batch_date: str) -> List[Dict[str, Any]]:
        try:
            json_match = re.search(r'\{.*\}', llm_response, re.DOTALL)
            if not json_match:
                logger.warning("No JSON found in LLM response")
                return []
            
            json_str = json_match.group()
            parsed_data = json.loads(json_str)
            
            topics_data = []
            review_id_map = {row.get('review_id', f'review_{idx}'): row for idx, row in reviews_chunk.iterrows()}
            
            for topic in parsed_data.get('topics', []):
                topic_name = topic.get('topic_name', '').strip()
                category = topic.get('category', 'issue')
                review_ids = topic.get('review_ids', [])
                
                if not topic_name:
                    continue
                
                for review_id in review_ids:
                    if review_id in review_id_map:
                        row = review_id_map[review_id]
                        topic_data = {
                            'review_id': review_id,
                            'topic_name': topic_name,
                            'topic_category': category,
                            'date': row['date'],
                            'batch_date': batch_date,
                            'is_seed_topic': any(seed.lower() in topic_name.lower() for seed in self.seed_topics),
                            'is_new_topic': topic.get('is_new', False)
                        }
                        topics_data.append(topic_data)
            
            return topics_data
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON parsing error: {e}")
            return []
        except Exception as e:
            logger.error(f"❌ Error parsing LLM response: {e}")
            return []