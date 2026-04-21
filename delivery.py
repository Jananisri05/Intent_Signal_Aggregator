import pandas as pd
import time
import requests
from hubspot import HubSpot

# =========================================================
# CONFIGURATION - FILL THESE IN
# =========================================================
# Copy this from your "Intent Signal Aggregator" Private App in HubSpot
HUBSPOT_ACCESS_TOKEN = "pat-na2-d6c8816c-64bb-4a1d-bf9b-98bb1798839f"

# Create a Slack App or use an Incoming Webhook URL
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"

# Path to your scoring output
CSV_FILE_PATH = "scored_accounts_precise.csv"

# INTERNAL NAMES (Must match exactly what you created in HubSpot)
FIELD_SCORE = "intent_score"
FIELD_SURGE = "is_intent_surge"
FIELD_SUMMARY = "intent_signal_summary"
# =========================================================

def sync_to_hubspot(client, df):
    print(f"\n--- Phase 1: Syncing {len(df)} accounts to HubSpot ---")
    success_count = 0
    skip_count = 0

    for _, row in df.iterrows():
        domain = row['domain']
        # Ensure data types are correct for the API
        score = float(row['score'])
        is_surge = bool(row['is_surge'])
        summary = f"Score: {score} | Total Signals: {row['total_signals']}"

        try:
            # 1. Search for company by domain
            search_result = client.crm.companies.search_api.do_search(
                public_object_search_request={
                    "filterGroups": [{
                        "filters": [{"propertyName": "domain", "operator": "EQ", "value": domain}]
                    }]
                }
            )

            if not search_result.results:
                print(f"  [!] Skip: No company found in CRM for {domain}")
                skip_count += 1
                continue

            company_id = search_result.results[0].id

            # 2. Update the company record
            client.crm.companies.basic_api.update(
                company_id, 
                properties={
                    FIELD_SCORE: score,
                    FIELD_SURGE: is_surge,
                    FIELD_SUMMARY: summary
                }
            )
            print(f"  [✓] Updated {domain} (Score: {score})")
            success_count += 1
            
            # Rate limiting protection
            time.sleep(0.1)

        except Exception as e:
            print(f"  [X] Error updating {domain}: {e}")

    print(f"\nCRM Sync Complete: {success_count} updated, {skip_count} skipped.")

def send_slack_alerts(df):
    print(f"\n--- Phase 2: Checking for Surges for Slack ---")
    
    # Filter only for rows where is_surge is True
    surges = df[df['is_surge'] == True]
    
    if surges.empty:
        print("No surge accounts detected. No alerts sent.")
        return

    for _, row in surges.iterrows():
        # Construct a professional Slack message
        payload = {
            "text": "🚨 *NEW INTENT SURGE DETECTED* 🚨",
            "attachments": [{
                "color": "#ff0000", # Red color for urgency
                "fields": [
                    {"title": "Company", "value": row['domain'], "short": True},
                    {"title": "Intent Score", "value": str(row['score']), "short": True},
                    {"title": "Total Signals", "value": str(row['total_signals']), "short": True},
                    {"title": "Action", "value": "Contact via CRM immediately!", "short": False}
                ]
            }]
        }

        try:
            response = requests.post(SLACK_WEBHOOK_URL, json=payload)
            if response.status_code == 200:
                print(f"  [✓] Slack alert sent for {row['domain']}")
            else:
                print(f"  [X] Slack error for {row['domain']}: {response.text}")
        except Exception as e:
            print(f"  [X] Failed to send Slack alert: {e}")

def main():
    print("🚀 STARTING INTENT SIGNAL DELIVERY PIPELINE 🚀")
    print("="*50)

    # Initialize HubSpot Client
    client = HubSpot(access_token=HUBSPOT_ACCESS_TOKEN)

    # Load Data
    try:
        df = pd.read_csv(CSV_FILE_PATH)
    except Exception as e:
        print(f"CRITICAL ERROR: Could not read CSV file: {e}")
        return

    # Run Delivery
    sync_to_hubspot(client, df)
    send_slack_alerts(df)

    print("\n" + "="*50)
    print("✅ PIPELINE FINISHED SUCCESSFULLY")
    print("="*50)

if __name__ == "__main__":
    main()
