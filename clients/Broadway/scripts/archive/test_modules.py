#!/usr/bin/env python3
"""
Test script for Broadway data modules.
Tests the cleanup and database loading functionality.
"""

import os
import sys
import pandas as pd
from data_cleanup import DataCleanup
from db_loader import DatabaseLoader
from db_connection import test_connection, get_table_counts

def test_data_cleanup():
    """Test the data cleanup module."""
    print("🧹 Testing Data Cleanup Module...")
    
    # Create a sample test CSV
    test_data = {
        'company_name': ['Camp Test 1', 'Camp Test 2', 'Camp Test 3'],
        'website_url': ['camptest1.com', 'https://camptest2.com', ''],
        'company_phone': ['555-1234', '(555) 567-8900', ''],
        'full_address': ['123 Test St', '456 Test Ave', ''],
        'city': ['Test City', 'Test Town', ''],
        'state': ['NY', 'CA', ''],
        'zip_code': ['12345', '67890', ''],
        'contact_name': ['John Doe', 'Jane Smith', ''],
        'contact_email': ['john@camptest1.com', 'jane@camptest2.com', ''],
        'contact_title': ['Director', 'Owner', ''],
        'contact_phone': ['555-1111', '555-2222', ''],
        'lat': [40.7128, 34.0522, None],
        'lon': [-74.0060, -118.2437, None],
        'rating': [4.5, 4.8, None],
        'review_count': [25, 42, None],
        'description': ['Great summer camp', 'Amazing experience', ''],
        'camp_type': ['Day Camp', 'Sleepaway', ''],
        'place_id': ['test_place_1', 'test_place_2', ''],
        'age_range': ['6-12', '8-16', ''],
        'session_length': ['2 weeks', '4 weeks', ''],
        'specialties': ['Sports', 'Arts', ''],
        # Add some columns that should be removed
        'gemini_enriched': [True, False, False],
        'enrichment_confidence': [0.8, 0.9, 0.0],
        'created_at': ['2025-01-01', '2025-01-02', '2025-01-03']
    }
    
    test_df = pd.DataFrame(test_data)
    test_csv_path = 'test_data.csv'
    test_df.to_csv(test_csv_path, index=False)
    
    print(f"Created test CSV with {len(test_df)} rows")
    
    # Test cleanup
    cleanup = DataCleanup()
    output_path = 'test_data_cleaned.csv'
    
    success = cleanup.process_csv(test_csv_path, output_path)
    
    if success:
        print("✅ Data cleanup test passed!")
        
        # Load cleaned data to verify
        cleaned_df = pd.read_csv(output_path)
        print(f"Cleaned CSV has {len(cleaned_df)} rows and {len(cleaned_df.columns)} columns")
        print(f"Columns: {list(cleaned_df.columns)}")
        
        # Clean up test files
        os.remove(test_csv_path)
        os.remove(output_path)
        
        return True
    else:
        print("❌ Data cleanup test failed!")
        return False

def test_database_connection():
    """Test database connection."""
    print("\n🗄️ Testing Database Connection...")
    
    success, message = test_connection()
    print(f"Status: {message}")
    
    if success:
        print("✅ Database connection test passed!")
        return True
    else:
        print("❌ Database connection test failed!")
        return False

def test_database_structure():
    """Test database structure."""
    print("\n🏗️ Testing Database Structure...")
    
    try:
        from db_connection import get_db_connection
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Check if our tables exist
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'summer_camps'
                    ORDER BY table_name;
                """)
                tables = cur.fetchall()
                
                if tables:
                    print("✅ Found tables:")
                    for table in tables:
                        print(f"  - {table[0]}")
                    
                    # Check table counts
                    counts = get_table_counts()
                    print("\nTable counts:")
                    for table, count in counts:
                        print(f"  {table}: {count} rows")
                    
                    return True
                else:
                    print("❌ No tables found in summer_camps schema!")
                    return False
                    
    except Exception as e:
        print(f"❌ Database structure test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Broadway Module Testing Suite")
    print("=" * 50)
    
    tests_passed = 0
    total_tests = 3
    
    # Test 1: Data Cleanup
    if test_data_cleanup():
        tests_passed += 1
    
    # Test 2: Database Connection
    if test_database_connection():
        tests_passed += 1
    
    # Test 3: Database Structure
    if test_database_structure():
        tests_passed += 1
    
    # Summary
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {tests_passed}/{total_tests} tests passed")
    
    if tests_passed == total_tests:
        print("🎉 All tests passed! Modules are ready for use.")
        return True
    else:
        print("⚠️ Some tests failed. Check the output above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
