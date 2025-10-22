import logging
import requests
import json
from typing import Optional
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from config import GROQ_API_KEY, GROQ_MODEL

logger = logging.getLogger(__name__)

class LLMClient:
    def __init__(self):
        self.api_key = GROQ_API_KEY
        self.model = GROQ_MODEL
        self.base_url = "https://api.groq.com/openai/v1/chat/completions"
        
        if not self.api_key or self.api_key == 'YOUR_GROQ_API_KEY_HERE':
            raise ValueError("❌ GROQ_API_KEY not set! Get free key from: https://console.groq.com/keys")
        
        logger.info(f"✅ Groq LLM Client initialized with model: {self.model}")
    
    def generate(self, prompt: str, max_tokens: int = 1000) -> str:
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": self.model,
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an expert at analyzing app reviews and extracting topics. Always respond with valid JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "temperature": 0.1,
                "max_tokens": max_tokens,
                "top_p": 0.9
            }
            
            response = requests.post(self.base_url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            return result['choices'][0]['message']['content'].strip()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Groq API error: {e}")
            return ""
        except Exception as e:
            logger.error(f"❌ Error generating response: {e}")
            return ""

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = LLMClient()
    test_response = client.generate("Extract topics from: 'Delivery was late and food was cold'")
    print(f"Test Response: {test_response}")