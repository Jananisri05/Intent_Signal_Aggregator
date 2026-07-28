# Intent Signal Aggregator

A comprehensive Python-based system for aggregating, scoring, and delivering intent signals from multiple data sources to identify high-value business opportunities.

## 📋 Overview

The Intent Signal Aggregator combines signals from three primary sources—**job postings**, **funding announcements**, and **news coverage**—to generate priority scores for companies showing strong buying intent. The system automates data collection, normalization, scoring, and delivery to your CRM and communication channels.

## 🎯 Key Features

- **Multi-Source Data Aggregation**: Combines job postings, funding data, and news signals
- **Intelligent Scoring Engine**: Weighted scoring with time-decay functionality
- **Surge Detection**: Identifies rapid increases in signal activity
- **CRM Integration**: Automatic sync with HubSpot for company records
- **Slack Alerts**: Real-time notifications for high-priority accounts
- **Modular Pipeline**: Easy to extend and customize each component

## 📦 Project Structure

```
Intent_Signal_Aggregator/
├── funding_data.py          # Extracts company funding information
├── job_collector.py         # Collects job posting signals
├── newssignals.py          # Aggregates news coverage signals
├── normalization.py        # Normalizes all signal sources
├── scoring_engine.py       # Calculates intent scores
├── delivery.py             # Syncs to HubSpot and Slack
└── upsert.py              # Database operations
```

## 🔄 Pipeline Flow

```
Data Collection
    ├── Funding Data → funding_data.py
    ├── Job Postings → job_collector.py
    └── News Coverage → newssignals.py
            ↓
    Normalization → normalization.py
            ↓
    Scoring Engine → scoring_engine.py
            ↓
    Delivery → delivery.py
            ├── HubSpot CRM Sync
            └── Slack Alerts
```

## 📊 Components

### 1. **funding_data.py** - Funding Information Extractor
Collects comprehensive company funding data from multiple sources:
- **Data Sources**:
  - Verified fallback data (pre-populated)
  - Apify Company Funding Tracker (SEC filings)
  - SEC-API.io (direct SEC EDGAR access)
  
- **Tracked Metrics**:
  - Total funding amount
  - Last round amount
  - Number of funding rounds
  - Funding type (Series A, B, C, IPO, etc.)
  - Last round date

- **Output**: `company_funding_complete.csv` and `company_funding_complete.json`

**Default Companies**: ClickUp, Airtable, Intercom, Mixpanel, Notion, Lattice, Loom, Benchling, Celonis, Docusign, BambooHR, Segment, Nexthink, Mirakl, Typeform, Productboard, Chargebee, Freshworks, Postman, Motive

### 2. **job_collector.py** - Job Posting Aggregator
Collects job postings as intent signals:
- Tracks job count per company
- Extracts company domain information
- Normalizes job posting data

### 3. **newssignals.py** - News Coverage Aggregator
Aggregates news mentions and press coverage:
- Monitors news articles about companies
- Extracts publication dates
- Normalizes domain information

### 4. **normalization.py** - Data Standardization
Standardizes all input data:
- Canonicalizes domain names (removes www, app, api prefixes)
- Standardizes date formats
- Ensures consistent schema across all data sources
- Outputs: `normalized_funding.csv`, `normalized_jobs.csv`, `normalized_news.csv`

### 5. **scoring_engine.py** - Intent Score Calculator
Advanced scoring algorithm with configurable weights:

**Scoring Configuration**:
```python
WEIGHTS = {
    'job': 40.0,      # Job postings weight
    'funding': 30.0,  # Funding signals weight
    'news': 30.0      # News coverage weight
}

DECAY_WINDOW_DAYS = 90        # Signal decay over 90 days
SURGE_THRESHOLD_DAYS = 7      # Surge window: 7 days
SURGE_SIGNAL_COUNT = 3        # 3+ signals in 7 days = surge
```

**Scoring Features**:
- Time-decay multiplier (older signals = lower scores)
- Normalized scores (0-100 scale)
- Surge detection (multiple signals in short timeframe)
- Signal aggregation per domain

**Output**: `scored_accounts_precise.csv` with columns:
- `domain`: Company domain
- `score`: Intent score (0-100)
- `is_surge`: Boolean indicating surge activity
- `total_signals`: Total signal count

### 6. **delivery.py** - CRM & Alert Integration
Delivers scored results to your business systems:

**Phase 1: HubSpot CRM Sync**
- Searches companies by domain
- Updates custom fields:
  - `intent_score`: Numerical score
  - `is_intent_surge`: Boolean for surge alerts
  - `intent_signal_summary`: Human-readable summary
- Rate-limited API calls (0.1s delay)

**Phase 2: Slack Notifications**
- Filters accounts with surge activity
- Sends formatted alerts with:
  - Company domain
  - Intent score
  - Total signals
  - Action recommendations
- Professional red-alert formatting

**Configuration Required**:
```python
HUBSPOT_ACCESS_TOKEN = "your-pat-token"
SLACK_WEBHOOK_URL = "your-webhook-url"
FIELD_SCORE = "intent_score"           # HubSpot custom field name
FIELD_SURGE = "is_intent_surge"        # HubSpot custom field name
FIELD_SUMMARY = "intent_signal_summary" # HubSpot custom field name
```

### 7. **upsert.py** - Database Operations
Handles database record updates:
- Upsert logic for efficient updates
- Batch processing support

## 🚀 Getting Started

### Prerequisites
```bash
pip install pandas requests hubspot
```

### Step 1: Collect Data
```bash
# Extract funding data
python funding_data.py

# Collect job signals
python job_collector.py

# Aggregate news signals
python newssignals.py
```

### Step 2: Normalize Data
```bash
python normalization.py
```

Outputs:
- `normalized_funding.csv`
- `normalized_jobs.csv`
- `normalized_news.csv`

### Step 3: Calculate Scores
```bash
python scoring_engine.py
```

Output:
- `scored_accounts_precise.csv`

### Step 4: Deliver Results
```bash
python delivery.py
```

This will:
1. Sync all accounts to HubSpot
2. Send Slack alerts for surge accounts
3. Generate completion report

### Output 
<img width="1917" height="812" alt="slack-1" src="https://github.com/user-attachments/assets/7c0ef347-05d9-42b7-a0ef-d4bea53bbe7b" />
<img width="1906" height="866" alt="slack-2" src="https://github.com/user-attachments/assets/b7777397-579d-4e10-b1f3-bd80a3633b44" />
<img width="1901" height="786" alt="slack-3" src="https://github.com/user-attachments/assets/efaa2ec3-17d0-46b6-a012-7d548ae3390f" />
<img width="1917" height="730" alt="slack-4" src="https://github.com/user-attachments/assets/3da50e5c-9aed-4e6f-a2f7-2c2f442ba33c" />


## 🔑 Configuration Guide

### HubSpot Setup
1. Create a Private App in HubSpot → Settings → Private Apps
2. Grant permissions: `crm.objects.companies.read` and `crm.objects.companies.write`
3. Copy the access token and update `delivery.py`

### Slack Setup
1. Create an Incoming Webhook in your Slack workspace
2. Choose a channel for alerts
3. Copy the webhook URL and update `delivery.py`

### Funding Data APIs
- **Apify**: Get free token at https://console.apify.com
- **SEC-API**: Get free token at https://sec-api.io

## 📈 Understanding Scores

**Score Calculation**:
- Base score from weighted signals
- Time decay: signals older than 90 days have reduced weight
- Max score: 100
- Higher scores = more recent/intense activity

**Surge Detection**:
- Triggered when 3+ signals occur within 7 days
- Indicates rapid company expansion activity
- Priority for sales outreach

**Example Scoring**:
```
Company: Acme Corp
- 4 job postings (40 * 0.8 decay) = 32 points
- 1 funding round (30 * 0.5 decay) = 15 points
- 2 news mentions (30 * 0.9 decay) = 27 points
Total Score: 74

Recent activity (past 7 days): 3+ signals = SURGE ✓
```

## 📝 Output Files

| File | Purpose | Format |
|------|---------|--------|
| `company_funding_complete.csv` | Funding data results | CSV |
| `company_funding_complete.json` | Funding data results | JSON |
| `normalized_funding.csv` | Standardized funding data | CSV |
| `normalized_jobs.csv` | Standardized job signals | CSV |
| `normalized_news.csv` | Standardized news signals | CSV |
| `scored_accounts_precise.csv` | Final scored results | CSV |

## 🛠️ Customization

### Adjust Scoring Weights
Edit `scoring_engine.py`:
```python
WEIGHTS = {
    'job': 50.0,      # Increase job importance
    'funding': 25.0,  # Decrease funding importance
    'news': 25.0      # Adjust news weight
}
```

### Change Surge Detection
```python
SURGE_THRESHOLD_DAYS = 14    # Longer window (14 days)
SURGE_SIGNAL_COUNT = 5       # Require 5+ signals
```

### Modify Decay Window
```python
DECAY_WINDOW_DAYS = 120      # Signals decay over 120 days instead of 90
```

## 🔐 Security Notes

⚠️ **Important**: Never commit API keys to version control!
- Use environment variables for sensitive credentials
- Use `.gitignore` to exclude config files
- Rotate API tokens regularly

**Example with environment variables**:
```python
import os
HUBSPOT_ACCESS_TOKEN = os.getenv('HUBSPOT_TOKEN')
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK')
```

## 📊 Example Output

**Console Output**:
```
🚀 STARTING INTENT SIGNAL DELIVERY PIPELINE 🚀
==================================================

--- Phase 1: Syncing 25 accounts to HubSpot ---
  [✓] Updated acme.com (Score: 78)
  [✓] Updated techcorp.com (Score: 85)
  [!] Skip: No company found in CRM for startup.io

CRM Sync Complete: 24 updated, 1 skipped.

--- Phase 2: Checking for Surges for Slack ---
  [✓] Slack alert sent for techcorp.com

==================================================
✅ PIPELINE FINISHED SUCCESSFULLY
==================================================
```

## 🐛 Troubleshooting

**No data found in funding extraction**:
- Verify API tokens are valid
- Check company names are spelled correctly
- Ensure rate limits aren't exceeded

**HubSpot sync fails**:
- Verify access token is correct
- Check custom field names match exactly
- Ensure company domains exist in HubSpot

**Slack alerts not sending**:
- Verify webhook URL is correct
- Check Slack app has permission to post
- Test webhook manually with curl

## 📚 Dependencies

- `pandas`: Data manipulation and CSV processing
- `requests`: HTTP requests for API calls
- `hubspot`: Official HubSpot Python SDK
- `tqdm`: Progress bar for scoring

## 📄 License

[Add your license here]

## 👥 Contributing

[Add contribution guidelines here]

## 📞 Support

For issues or questions, please open a GitHub issue or contact the repository maintainer.

---

**Last Updated**: April 2026  
**Language**: Python 3.8+  
**Status**: Active Development
