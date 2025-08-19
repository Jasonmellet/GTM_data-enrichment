#!/usr/bin/env python3
"""
ZeroBounce Validator Module - Email Validation Integration

This module provides the ZeroBounceValidator class for email validation.
"""

import os
import logging
import aiohttp
from typing import Dict, List
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

class ZeroBounceValidator:
    """Validates emails using ZeroBounce API."""
    
    def __init__(self):
        self.api_key = os.getenv('BROADWAY_ZEROBOUNCE_API_KEY')
        if not self.api_key:
            raise ValueError("BROADWAY_ZEROBOUNCE_API_KEY environment variable not set")
        
        self.base_url = "https://api.zerobounce.net/v2"
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def validate_single_email(self, email: str) -> Dict:
        """Validate a single email address."""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        url = f"{self.base_url}/validate"
        params = {
            'api_key': self.api_key,
            'email': email,
            'ip_address': ''  # Required parameter (can be blank but must be present)
        }
        
        try:
            logger.info(f"Calling ZeroBounce API: {url} with params: {params}")
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(f"Validation result for {email}: {result.get('status', 'unknown')} - Full response: {result}")
                    return result
                else:
                    error_text = await response.text()
                    logger.error(f"ZeroBounce API error: {response.status} - {error_text}")
                    logger.error(f"Failed URL: {response.url}")
                    return {'error': f"API error: {response.status}", 'details': error_text}
                    
        except Exception as e:
            logger.error(f"Error validating {email}: {e}")
            return {'error': str(e)}
    
    def parse_validation_result(self, result: Dict) -> Dict:
        """Parse and standardize validation result."""
        if 'error' in result:
            return {
                'status': 'error',
                'score': 0,
                'risk_score': 100,
                'details': result
            }
        
        # Map ZeroBounce status to our standard format
        status_mapping = {
            'valid': 'valid',
            'invalid': 'invalid',
            'catch-all': 'catch_all',
            'disposable': 'disposable',
            'unknown': 'unknown',
            'spamtrap': 'spamtrap',
            'abuse': 'abuse',
            'dont_send': 'dont_send'
        }
        
        status = status_mapping.get(result.get('status', 'unknown'), 'unknown')
        score = result.get('score', 0)
        risk_score = result.get('risk_score', 100)
        
        return {
            'status': status,
            'score': score,
            'risk_score': risk_score,
            'details': result
        }
