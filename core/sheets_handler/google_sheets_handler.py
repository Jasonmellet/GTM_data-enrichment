import os
import pandas as pd
from typing import List, Dict, Optional
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pickle
from config import GOOGLE_SHEETS_CREDENTIALS_FILE, GOOGLE_SHEETS_TOKEN_FILE

class GoogleSheetsHandler:
    """Handles Google Sheets operations for reading and writing business data."""
    
    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    def __init__(self):
        self.creds = None
        self.service = None
        self.authenticate()
    
    def authenticate(self):
        """Authenticate with Google Sheets API."""
        # The file token.json stores the user's access and refresh tokens.
        if os.path.exists(GOOGLE_SHEETS_TOKEN_FILE):
            with open(GOOGLE_SHEETS_TOKEN_FILE, 'rb') as token:
                self.creds = pickle.load(token)
        
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                if not os.path.exists(GOOGLE_SHEETS_CREDENTIALS_FILE):
                    raise FileNotFoundError(
                        f"Credentials file '{GOOGLE_SHEETS_CREDENTIALS_FILE}' not found. "
                        "Please download it from Google Cloud Console."
                    )
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    GOOGLE_SHEETS_CREDENTIALS_FILE, self.SCOPES)
                # Use a fixed port to avoid redirect URI issues
                self.creds = flow.run_local_server(port=8080, prompt='consent')
            
            # Save the credentials for the next run
            with open(GOOGLE_SHEETS_TOKEN_FILE, 'wb') as token:
                pickle.dump(self.creds, token)
        
        self.service = build('sheets', 'v4', credentials=self.creds)
    
    def extract_sheet_id_from_url(self, url: str) -> str:
        """Extract Google Sheet ID from URL."""
        # Handle different Google Sheets URL formats
        if '/spreadsheets/d/' in url:
            # Format: https://docs.google.com/spreadsheets/d/SHEET_ID/edit
            start = url.find('/spreadsheets/d/') + 17
            end = url.find('/', start)
            if end == -1:
                end = url.find('?', start)
            if end == -1:
                end = url.find('#', start)
            if end == -1:
                end = len(url)
            sheet_id = url[start:end]
            print(f"Extracted sheet ID: {sheet_id}")
            return sheet_id
        elif 'id=' in url:
            # Format: https://docs.google.com/spreadsheets/d/SHEET_ID/edit#gid=0
            start = url.find('id=') + 3
            end = url.find('&', start)
            if end == -1:
                end = url.find('#', start)
            if end == -1:
                end = len(url)
            sheet_id = url[start:end]
            print(f"Extracted sheet ID: {sheet_id}")
            return sheet_id
        else:
            # Try to extract from the URL path directly
            parts = url.split('/')
            for i, part in enumerate(parts):
                if part == 'd' and i + 1 < len(parts):
                    sheet_id = parts[i + 1]
                    print(f"Extracted sheet ID from path: {sheet_id}")
                    return sheet_id
            # If it's just the ID, return it directly
            if len(url) > 20 and all(c.isalnum() or c in '-_' for c in url):
                print(f"Using URL as sheet ID: {url}")
                return url
            raise ValueError("Could not extract Sheet ID from URL. Please provide a valid Google Sheets URL.")
    
    def read_sheet_data(self, sheet_url: str, sheet_name: str = None) -> pd.DataFrame:
        """Read data from Google Sheet and return as pandas DataFrame."""
        sheet_id = self.extract_sheet_id_from_url(sheet_url)
        
        try:
            # Get sheet metadata to find sheet names
            sheet_metadata = self.service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
            
            if not sheet_name:
                # Use first sheet if none specified
                sheet_name = sheet_metadata['sheets'][0]['properties']['title']
            
            # Read the data with a wider range to handle dynamic columns
            result = self.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=f"{sheet_name}!A1:ZZ1000"  # Wide range to capture all columns
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                raise ValueError(f"No data found in sheet '{sheet_name}'")
            
            # Debug: Check the data structure
            print(f"📊 Raw data: {len(values)} rows")
            print(f"📊 Headers: {len(values[0])} columns")
            print(f"📊 First few headers: {values[0][:5]}")
            
            # Ensure all rows have the same number of columns
            max_cols = max(len(row) for row in values)
            padded_values = []
            for row in values:
                # Pad shorter rows with empty strings
                padded_row = row + [''] * (max_cols - len(row))
                padded_values.append(padded_row)
            
            print(f"📊 Padded data: {len(padded_values)} rows, {len(padded_values[0])} columns")
            
            # Convert to DataFrame
            df = pd.DataFrame(padded_values[1:], columns=padded_values[0])
            return df
            
        except HttpError as error:
            print(f"An error occurred: {error}")
            raise
    
    def _get_column_letter(self, column_index: int) -> str:
        """Convert column index to Excel column letter (A, B, C, ..., Z, AA, AB, etc.)."""
        result = ""
        while column_index > 0:
            column_index, remainder = divmod(column_index - 1, 26)
            result = chr(65 + remainder) + result
        return result

    def write_naics_codes(self, sheet_url: str, sheet_name: str, 
                         enriched_data: List[Dict], naics_column: str = 'NAICS_Code') -> bool:
        """Write NAICS codes and Likely to Buy analysis to specific columns AT and AU."""
        sheet_id = self.extract_sheet_id_from_url(sheet_url)
        
        try:
            # First, let's read the current headers to see the actual column count
            result = self.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=f"{sheet_name}!A1:ZZ1"  # Read header row with more columns
            ).execute()
            
            headers = result.get('values', [[]])[0]
            print(f"📊 Found {len(headers)} columns in the sheet")
            
            # Find columns AT and AU by name or position
            naics_col_index = None
            likely_to_buy_col_index = None
            
            # Look for existing columns first
            for i, header in enumerate(headers):
                if header and 'naics' in header.lower():
                    naics_col_index = i
                    print(f"📝 Found existing NAICS column at position {self._get_column_letter(naics_col_index)}")
                    break
            
            for i, header in enumerate(headers):
                if header and 'likely to buy' in header.lower():
                    likely_to_buy_col_index = i
                    print(f"📝 Found existing Likely to Buy column at position {self._get_column_letter(likely_to_buy_col_index)}")
                    break
            
            # If not found, use the expected positions (AT and AU)
            if naics_col_index is None:
                naics_col_index = 45  # Column AT (0-indexed: A=0, B=1, ..., AT=45)
                print(f"📝 Using column AT ({self._get_column_letter(naics_col_index)}) for NAICS codes")
            
            if likely_to_buy_col_index is None:
                likely_to_buy_col_index = 46  # Column AU (0-indexed: A=0, B=1, ..., AU=46)
                print(f"📝 Using column AU ({self._get_column_letter(likely_to_buy_col_index)}) for Likely to Buy analysis")
            
            # Write headers to first row
            print(f"📝 Writing column headers...")
            
            # Write NAICS header to column AT, row 1
            naics_header_range = f"{sheet_name}!{self._get_column_letter(naics_col_index)}1"
            self.service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=naics_header_range,
                valueInputOption='RAW',
                body={'values': [['NAICS Code']]}
            ).execute()
            
            # Write Likely to Buy header to column AU, row 1
            likely_header_range = f"{sheet_name}!{self._get_column_letter(likely_to_buy_col_index)}1"
            self.service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=likely_header_range,
                valueInputOption='RAW',
                body={'values': [['Likely to Buy']]}
            ).execute()
            
            print(f"✅ Added headers: 'NAICS Code' in column {self._get_column_letter(naics_col_index)}, 'Likely to Buy' in column {self._get_column_letter(likely_to_buy_col_index)}")
            
            # Prepare data for writing
            naics_values = []
            likely_to_buy_values = []
            
            for row_data in enriched_data:
                if 'naics_code' in row_data and row_data['naics_code']:
                    naics_values.append([row_data['naics_code']])
                else:
                    naics_values.append([''])
                
                if 'likely_to_buy' in row_data and row_data['likely_to_buy']:
                    likely_to_buy_values.append([row_data['likely_to_buy']])
                else:
                    likely_to_buy_values.append([''])
            
            # Write NAICS codes to column AT
            if naics_values:
                range_name = f"{sheet_name}!{self._get_column_letter(naics_col_index)}2:{self._get_column_letter(naics_col_index)}{len(naics_values) + 1}"
                self.service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=range_name,
                    valueInputOption='RAW',
                    body={'values': naics_values}
                ).execute()
                print(f"✅ Wrote NAICS codes to column {self._get_column_letter(naics_col_index)}")
            
            # Write Likely to Buy analysis to column AU
            if likely_to_buy_values:
                range_name = f"{sheet_name}!{self._get_column_letter(likely_to_buy_col_index)}2:{self._get_column_letter(likely_to_buy_col_index)}{len(likely_to_buy_values) + 1}"
                self.service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=range_name,
                    valueInputOption='RAW',
                    body={'values': likely_to_buy_values}
                ).execute()
                print(f"✅ Wrote Likely to Buy analysis to column {self._get_column_letter(likely_to_buy_col_index)}")
            
            return True
            
        except HttpError as error:
            print(f"An error occurred while writing: {error}")
            return False
    
    def get_sheet_names(self, sheet_url: str) -> List[str]:
        """Get list of sheet names from Google Sheet."""
        sheet_id = self.extract_sheet_id_from_url(sheet_url)
        
        try:
            sheet_metadata = self.service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
            
            sheet_names = [sheet['properties']['title'] for sheet in sheet_metadata['sheets']]
            return sheet_names
            
        except HttpError as error:
            print(f"An error occurred: {error}")
            return []
