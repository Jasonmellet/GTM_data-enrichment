#!/usr/bin/env python3
"""Check which columns in the contacts table are actually being used."""

from db_connection import get_db_connection

def check_column_usage():
    """Analyze column usage in the contacts table."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Get total count
            cur.execute('SELECT COUNT(*) FROM summer_camps.contacts')
            total = cur.fetchone()[0]
            print(f'Total contacts: {total}')
            
            # Check potentially unused columns
            columns_to_check = [
                'predicted_email',
                'email_prediction_confidence', 
                'email_prediction_timestamp',
                'email_prediction_method',
                'email_validation_risk_score',
                'email_validation_details'
            ]
            
            print('\nColumn Usage Analysis:')
            print('Column | Non-Null Count | Usage %')
            print('-' * 50)
            
            for col in columns_to_check:
                cur.execute(f'SELECT COUNT(*) FROM summer_camps.contacts WHERE {col} IS NOT NULL')
                count = cur.fetchone()[0]
                usage = (count/total)*100 if total > 0 else 0
                print(f'{col:<25} | {count:<13} | {usage:.1f}%')

if __name__ == '__main__':
    check_column_usage()
