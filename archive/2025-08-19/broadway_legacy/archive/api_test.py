#!/usr/bin/env python3
"""
API Test Script for Broadway

Tests all API integrations to verify they are working correctly:
- Perplexity API
- Google Maps API
- Apollo API
- Yelp API

For each API, performs a simple test call and reports:
- Authentication status
- Response format
- Rate limits
- Sample data
- Cost estimate (where applicable)
"""

import os
import sys
import json
import time
import requests
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

# Add the project root to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

# Import environment variables
from dotenv import load_dotenv
load_dotenv()

# ANSI color codes for terminal output
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
BOLD = "\033[1m"
RESET = "\033[0m"

class APITester:
    def __init__(self):
        self.results = {
            "perplexity": {"status": "not_tested", "details": {}},
            "google_maps": {"status": "not_tested", "details": {}},
            "apollo": {"status": "not_tested", "details": {}},
            "yelp": {"status": "not_tested", "details": {}}
        }
        self.cost_estimates = {
            "perplexity": 0.0,
            "google_maps": 0.0,
            "apollo": 0.0,
            "yelp": 0.0
        }
    
    def print_header(self, text: str) -> None:
        """Print a formatted header."""
        print(f"\n{BOLD}{BLUE}{'=' * 50}{RESET}")
        print(f"{BOLD}{BLUE}{text.center(50)}{RESET}")
        print(f"{BOLD}{BLUE}{'=' * 50}{RESET}\n")
    
    def print_result(self, api_name: str, success: bool, message: str) -> None:
        """Print a formatted result."""
        status = f"{GREEN}✓ PASS{RESET}" if success else f"{RED}✗ FAIL{RESET}"
        print(f"{status} {api_name}: {message}")
    
    def print_warning(self, message: str) -> None:
        """Print a warning message."""
        print(f"{YELLOW}⚠️  {message}{RESET}")
    
    def print_info(self, message: str) -> None:
        """Print an info message."""
        print(f"{BLUE}ℹ️  {message}{RESET}")
    
    def format_json(self, data: Any) -> str:
        """Format JSON data for display."""
        return json.dumps(data, indent=2, sort_keys=True)
    
    def test_perplexity(self) -> bool:
        """Test the Perplexity API."""
        self.print_header("Testing Perplexity API")
        
        api_key = os.getenv("BROADWAY_PERPLEXITY_API_KEY") or os.getenv("PERPLEXITY_API_KEY")
        if not api_key:
            self.print_result("Perplexity", False, "API key not found in environment variables")
            self.results["perplexity"]["status"] = "failed"
            self.results["perplexity"]["details"]["error"] = "API key not found"
            return False
        
        # Test with a simple query
        query = "What are the top summer camps in the United States? Provide a brief JSON response with 3 examples."
        
        try:
            start_time = time.time()
            response = requests.post(
                "https://api.perplexity.ai/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "sonar",  # Try with default model
                    "messages": [
                        {"role": "system", "content": "You are a helpful assistant that responds in JSON format."},
                        {"role": "user", "content": query}
                    ]
                },
                timeout=30
            )
            elapsed = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract usage information
                usage = data.get("usage", {})
                cost = usage.get("cost", {}).get("total_cost", 0.0)
                self.cost_estimates["perplexity"] = cost
                
                # Extract response content
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                
                # Log details
                self.results["perplexity"]["status"] = "success"
                self.results["perplexity"]["details"] = {
                    "model": "sonar",
                    "response_time": f"{elapsed:.2f}s",
                    "token_usage": usage,
                    "cost": cost,
                    "rate_limits": response.headers.get("X-RateLimit-Remaining", "Unknown")
                }
                
                self.print_result("Perplexity", True, f"API call successful in {elapsed:.2f}s")
                self.print_info(f"Model: sonar")
                self.print_info(f"Cost: ${cost}")
                
                # Try to parse JSON from the response
                try:
                    # Find JSON in the response if it exists
                    import re
                    json_match = re.search(r'(\{.*\})', content, re.DOTALL)
                    if json_match:
                        json_content = json.loads(json_match.group(1))
                        print(f"\nSample response (parsed JSON):\n{self.format_json(json_content)[:500]}...\n")
                    else:
                        print(f"\nSample response (text):\n{content[:500]}...\n")
                except Exception as e:
                    print(f"\nSample response (text):\n{content[:500]}...\n")
                
                return True
            else:
                self.print_result("Perplexity", False, f"API call failed with status {response.status_code}")
                self.print_warning(f"Response: {response.text}")
                
                self.results["perplexity"]["status"] = "failed"
                self.results["perplexity"]["details"] = {
                    "status_code": response.status_code,
                    "error": response.text
                }
                return False
                
        except Exception as e:
            self.print_result("Perplexity", False, f"Exception: {str(e)}")
            self.results["perplexity"]["status"] = "failed"
            self.results["perplexity"]["details"]["error"] = str(e)
            return False
    
    def test_google_maps(self) -> bool:
        """Test the Google Maps API."""
        self.print_header("Testing Google Maps API")
        
        api_key = os.getenv("BROADWAY_GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_MAPS_API_KEY")
        if not api_key:
            self.print_result("Google Maps", False, "API key not found in environment variables")
            self.results["google_maps"]["status"] = "failed"
            self.results["google_maps"]["details"]["error"] = "API key not found"
            return False
        
        # Test with a simple place search
        query = "Camp Chateaugay"
        
        try:
            # First test: Find Place API
            start_time = time.time()
            find_place_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
            find_place_params = {
                "input": query,
                "inputtype": "textquery",
                "fields": "place_id,name,formatted_address,business_status",
                "key": api_key
            }
            
            find_response = requests.get(find_place_url, params=find_place_params, timeout=10)
            find_elapsed = time.time() - start_time
            
            if find_response.status_code != 200:
                self.print_result("Google Maps (Find Place)", False, f"API call failed with status {find_response.status_code}")
                self.print_warning(f"Response: {find_response.text}")
                self.results["google_maps"]["status"] = "failed"
                self.results["google_maps"]["details"] = {
                    "status_code": find_response.status_code,
                    "error": find_response.text
                }
                return False
            
            find_data = find_response.json()
            self.print_result("Google Maps (Find Place)", True, f"API call successful in {find_elapsed:.2f}s")
            
            # Calculate approximate cost
            find_place_cost = 0.017  # $0.017 per request
            self.cost_estimates["google_maps"] += find_place_cost
            
            # Second test: Place Details API (if we found a place)
            place_id = None
            if find_data.get("candidates") and len(find_data["candidates"]) > 0:
                place_id = find_data["candidates"][0].get("place_id")
            
            if place_id:
                start_time = time.time()
                details_url = "https://maps.googleapis.com/maps/api/place/details/json"
                details_params = {
                    "place_id": place_id,
                    "fields": "name,formatted_phone_number,formatted_address,website,business_status",
                    "key": api_key
                }
                
                details_response = requests.get(details_url, params=details_params, timeout=10)
                details_elapsed = time.time() - start_time
                
                if details_response.status_code == 200:
                    details_data = details_response.json()
                    self.print_result("Google Maps (Place Details)", True, f"API call successful in {details_elapsed:.2f}s")
                    
                    # Calculate approximate cost
                    details_cost = 0.017  # $0.017 per request
                    self.cost_estimates["google_maps"] += details_cost
                    
                    self.results["google_maps"]["status"] = "success"
                    self.results["google_maps"]["details"] = {
                        "find_place": {
                            "response_time": f"{find_elapsed:.2f}s",
                            "cost": find_place_cost,
                            "status": find_data.get("status")
                        },
                        "place_details": {
                            "response_time": f"{details_elapsed:.2f}s",
                            "cost": details_cost,
                            "status": details_data.get("status")
                        },
                        "total_cost": find_place_cost + details_cost
                    }
                    
                    self.print_info(f"Total cost: ${find_place_cost + details_cost:.3f}")
                    
                    # Show sample data
                    if details_data.get("result"):
                        result = details_data["result"]
                        print(f"\nSample data for {result.get('name', 'Unknown')}:")
                        print(f"  Address: {result.get('formatted_address', 'N/A')}")
                        print(f"  Phone: {result.get('formatted_phone_number', 'N/A')}")
                        print(f"  Website: {result.get('website', 'N/A')}")
                        print(f"  Business Status: {result.get('business_status', 'N/A')}\n")
                    
                    return True
                else:
                    self.print_result("Google Maps (Place Details)", False, f"API call failed with status {details_response.status_code}")
                    self.print_warning(f"Response: {details_response.text}")
                    
                    self.results["google_maps"]["status"] = "partial_success"
                    self.results["google_maps"]["details"] = {
                        "find_place": {
                            "response_time": f"{find_elapsed:.2f}s",
                            "cost": find_place_cost,
                            "status": find_data.get("status")
                        },
                        "place_details": {
                            "status_code": details_response.status_code,
                            "error": details_response.text
                        },
                        "total_cost": find_place_cost
                    }
                    return False
            else:
                self.print_warning("No place found, skipping Place Details API test")
                
                self.results["google_maps"]["status"] = "partial_success"
                self.results["google_maps"]["details"] = {
                    "find_place": {
                        "response_time": f"{find_elapsed:.2f}s",
                        "cost": find_place_cost,
                        "status": find_data.get("status"),
                        "message": "No place found"
                    },
                    "total_cost": find_place_cost
                }
                
                self.print_info(f"Cost: ${find_place_cost:.3f}")
                return True
                
        except Exception as e:
            self.print_result("Google Maps", False, f"Exception: {str(e)}")
            self.results["google_maps"]["status"] = "failed"
            self.results["google_maps"]["details"]["error"] = str(e)
            return False
    
    def test_apollo(self) -> bool:
        """Test the Apollo API."""
        self.print_header("Testing Apollo API")
        
        api_key = os.getenv("BROADWAY_APOLLO_API_KEY")
        if not api_key:
            self.print_result("Apollo", False, "API key not found in environment variables")
            self.results["apollo"]["status"] = "failed"
            self.results["apollo"]["details"]["error"] = "API key not found"
            return False
        
        # Test with a simple person search
        try:
            start_time = time.time()
            
            # Based on the error we saw earlier, use the X-Api-Key header instead of query param
            headers = {
                "X-Api-Key": api_key,
                "Content-Type": "application/json"
            }
            
            data = {
                "q_person_name": "Mitch Goldman",
                "page": 1,
                "per_page": 1
            }
            
            response = requests.post(
                "https://api.apollo.io/v1/people/search",
                headers=headers,
                json=data,
                timeout=10
            )
            
            elapsed = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract rate limit information
                rate_limit = {
                    "limit": response.headers.get("X-RateLimit-Limit", "Unknown"),
                    "remaining": response.headers.get("X-RateLimit-Remaining", "Unknown"),
                    "reset": response.headers.get("X-RateLimit-Reset", "Unknown")
                }
                
                # Approximate cost (Apollo pricing varies)
                estimated_cost = 0.10  # Placeholder cost per lookup
                self.cost_estimates["apollo"] = estimated_cost
                
                self.results["apollo"]["status"] = "success"
                self.results["apollo"]["details"] = {
                    "response_time": f"{elapsed:.2f}s",
                    "rate_limits": rate_limit,
                    "estimated_cost": estimated_cost
                }
                
                self.print_result("Apollo", True, f"API call successful in {elapsed:.2f}s")
                self.print_info(f"Rate limits - Remaining: {rate_limit['remaining']}, Limit: {rate_limit['limit']}")
                self.print_info(f"Estimated cost: ${estimated_cost}")
                
                # Show sample data
                if data.get("people") and len(data["people"]) > 0:
                    person = data["people"][0]
                    print(f"\nSample data for {person.get('name', 'Unknown')}:")
                    print(f"  Email: {person.get('email', 'N/A')}")
                    print(f"  Title: {person.get('title', 'N/A')}")
                    print(f"  Company: {person.get('organization', {}).get('name', 'N/A')}")
                    print(f"  LinkedIn: {person.get('linkedin_url', 'N/A')}\n")
                else:
                    print("\nNo matching people found\n")
                
                return True
            else:
                self.print_result("Apollo", False, f"API call failed with status {response.status_code}")
                self.print_warning(f"Response: {response.text}")
                
                self.results["apollo"]["status"] = "failed"
                self.results["apollo"]["details"] = {
                    "status_code": response.status_code,
                    "error": response.text
                }
                return False
                
        except Exception as e:
            self.print_result("Apollo", False, f"Exception: {str(e)}")
            self.results["apollo"]["status"] = "failed"
            self.results["apollo"]["details"]["error"] = str(e)
            return False
    
    def test_yelp(self) -> bool:
        """Test the Yelp API."""
        self.print_header("Testing Yelp API")
        
        api_key = os.getenv("BROADWAY_YELP_API_KEY")
        if not api_key:
            self.print_result("Yelp", False, "API key not found in environment variables")
            self.results["yelp"]["status"] = "failed"
            self.results["yelp"]["details"]["error"] = "API key not found"
            return False
        
        # Test with a simple business search
        try:
            start_time = time.time()
            
            headers = {
                "Authorization": f"Bearer {api_key}"
            }
            
            params = {
                "term": "Camp Chateaugay",
                "location": "Merrill, NY",
                "limit": 1
            }
            
            response = requests.get(
                "https://api.yelp.com/v3/businesses/search",
                headers=headers,
                params=params,
                timeout=10
            )
            
            elapsed = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract rate limit information
                rate_limit = {
                    "daily": response.headers.get("RateLimit-DailyLimit", "Unknown"),
                    "remaining": response.headers.get("RateLimit-Remaining", "Unknown"),
                    "reset": response.headers.get("RateLimit-ResetTime", "Unknown")
                }
                
                # Yelp API is free but has rate limits
                estimated_cost = 0.00
                
                self.results["yelp"]["status"] = "success"
                self.results["yelp"]["details"] = {
                    "response_time": f"{elapsed:.2f}s",
                    "rate_limits": rate_limit
                }
                
                self.print_result("Yelp", True, f"API call successful in {elapsed:.2f}s")
                
                # Show sample data
                if data.get("businesses") and len(data["businesses"]) > 0:
                    business = data["businesses"][0]
                    print(f"\nSample data for {business.get('name', 'Unknown')}:")
                    print(f"  Rating: {business.get('rating', 'N/A')}")
                    print(f"  Phone: {business.get('display_phone', 'N/A')}")
                    print(f"  Address: {', '.join(business.get('location', {}).get('display_address', ['N/A']))}")
                    print(f"  Categories: {', '.join([c.get('title', '') for c in business.get('categories', [])])}\n")
                else:
                    print("\nNo matching businesses found\n")
                
                return True
            else:
                self.print_result("Yelp", False, f"API call failed with status {response.status_code}")
                self.print_warning(f"Response: {response.text}")
                
                self.results["yelp"]["status"] = "failed"
                self.results["yelp"]["details"] = {
                    "status_code": response.status_code,
                    "error": response.text
                }
                return False
                
        except Exception as e:
            self.print_result("Yelp", False, f"Exception: {str(e)}")
            self.results["yelp"]["status"] = "failed"
            self.results["yelp"]["details"]["error"] = str(e)
            return False
    
    def run_all_tests(self) -> None:
        """Run all API tests and print a summary."""
        perplexity_success = self.test_perplexity()
        google_maps_success = self.test_google_maps()
        apollo_success = self.test_apollo()
        yelp_success = self.test_yelp()
        
        self.print_header("API Test Summary")
        
        print(f"{BOLD}Test Results:{RESET}")
        for api, result in self.results.items():
            status = result["status"]
            if status == "success":
                print(f"  {GREEN}✓ {api.capitalize()}: Success{RESET}")
            elif status == "partial_success":
                print(f"  {YELLOW}⚠️ {api.capitalize()}: Partial Success{RESET}")
            elif status == "failed":
                print(f"  {RED}✗ {api.capitalize()}: Failed{RESET}")
            else:
                print(f"  {YELLOW}? {api.capitalize()}: Not Tested{RESET}")
        
        print(f"\n{BOLD}Cost Estimates:{RESET}")
        total_cost = sum(self.cost_estimates.values())
        for api, cost in self.cost_estimates.items():
            print(f"  {api.capitalize()}: ${cost:.3f}")
        print(f"  {BOLD}Total: ${total_cost:.3f}{RESET}")
        
        # Save results to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"../outputs/api_test_results_{timestamp}.json")
        
        output = {
            "timestamp": datetime.now().isoformat(),
            "results": self.results,
            "cost_estimates": self.cost_estimates,
            "total_cost": total_cost
        }
        
        os.makedirs(os.path.dirname(os.path.abspath(results_file)), exist_ok=True)
        with open(results_file, "w") as f:
            json.dump(output, f, indent=2)
        
        print(f"\n{BOLD}Results saved to: {results_file}{RESET}")

if __name__ == "__main__":
    tester = APITester()
    tester.run_all_tests()
