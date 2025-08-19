#!/usr/bin/env python3
"""
Test script to verify column letter conversion.
"""

from google_sheets_handler import GoogleSheetsHandler

def test_column_letters():
    """Test the column letter conversion function."""
    print("🔍 Testing Column Letter Conversion")
    print("=" * 40)
    
    handler = GoogleSheetsHandler()
    
    # Test some key column indices
    test_indices = [0, 1, 25, 26, 27, 45, 46, 47, 48, 49, 50]
    
    for index in test_indices:
        letter = handler._get_column_letter(index)
        print(f"  Column {index} → {letter}")
    
    print(f"\n✅ Column letter conversion test completed!")

if __name__ == "__main__":
    test_column_letters()
