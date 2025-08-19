# 🚀 Quick Setup Guide

Follow these steps to get your NAICS enrichment tool running in minutes!

## Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

## Step 2: Set Up Google Sheets API

1. **Go to [Google Cloud Console](https://console.cloud.google.com/)**
2. **Create a new project** (or select existing)
3. **Enable Google Sheets API**:
   - Go to "APIs & Services" → "Library"
   - Search for "Google Sheets API"
   - Click "Enable"
4. **Create credentials**:
   - Go to "APIs & Services" → "Credentials"
   - Click "Create Credentials" → "OAuth 2.0 Client IDs"
   - Choose "Desktop application"
   - Download the JSON file
5. **Rename the file** to `credentials.json` and place it in this directory

## Step 3: Get Free Government API Key (Optional but Recommended)

1. **Visit [Census Bureau API](https://api.census.gov/data/key_signup.html)**
2. **Sign up for free API key**
3. **Create `.env` file** in this directory:
   ```bash
   CENSUS_API_KEY=your_census_api_key_here
   ```

## Step 4: Set Up AI Services (Optional - for fallback)

If you want AI-powered classification when government APIs don't have data:

1. **OpenAI**: [Get API key](https://platform.openai.com/api-keys)
2. **Anthropic Claude**: [Get API key](https://console.anthropic.com/)
3. **Google Gemini**: [Get API key](https://makersuite.google.com/app/apikey)

Add to your `.env` file:
```bash
OPENAI_API_KEY=your_openai_api_key_here
ANTHROPIC_API_KEY=your_anthropic_api_key_here
GOOGLE_AI_API_KEY=your_google_ai_api_key_here
```

## Step 5: Test the Tool

1. **First run (will open browser for Google authentication)**:
   ```bash
   python main.py "YOUR_GOOGLE_SHEET_URL" --dry-run
   ```

2. **Check the output** - you should see:
   - ✅ Connection to Google Sheets
   - ✅ Data reading
   - 🔍 Processing businesses
   - 📊 Summary statistics

## Step 6: Run for Real

```bash
python main.py "YOUR_GOOGLE_SHEET_URL"
```

## 🎯 Your Google Sheet Should Look Like:

| Business Name | Industry | Description | Contact |
|---------------|----------|-------------|---------|
| Acme Corp | Technology | Software development | john@acme.com |
| Joe's Diner | Restaurant | Family restaurant | joe@diner.com |

## 🔧 Troubleshooting

### "Credentials file not found"
- Make sure `credentials.json` is in the same directory
- Check the filename spelling

### "Permission denied"
- Ensure your Google account has access to the sheet
- Check that Google Sheets API is enabled

### Browser opens for authentication
- This is normal on first run
- Sign in with the Google account that has access to your sheet

## 📞 Need Help?

- Check the console output for error messages
- Use `--dry-run` to test without modifying your sheet
- Review the full README.md for detailed documentation
