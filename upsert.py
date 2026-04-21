import pandas as pd
import time
import requests
from hubspot import HubSpot
# IMPORTANT: We must import this specific model to "wrap" our data
from hubspot.crm.companies.models import SimplePublicObjectInputForCreate

# =========================================================
# CONFIGURATION - FILL THESE IN
# =========================================================
HUBSPOT_ACCESS_TOKEN = "pat-na2-d6c8816c-64bb-4a1d-bf9b-98bb1798839f"
CSV_FILE_PATH = "scored_accounts_precise.csv"

# INTERNAL NAMES (Must match exactly what you created in HubSpot)
FIELD_DOMAIN = "domain" 
FIELD_SCORE = "intent_score"
FIELD_SURGE = "is_intent_surge"
FIELD_SUMMARY = "intent_signal_summary"
# =========================================================

def upsert_to_hubspot(client, df):
    print(f"\n--- Phase 1: Upserting {len(df)} accounts to HubSpot ---")
    success_count = 0
    created_count = 0
    error_count = 0

    for _, row in df.iterrows():
        domain = row['domain']
        score = float(row['score'])
        is_surge = bool(row['is_surge'])
        summary = f"Score: {score} | Total Signals: {row['total_signals']}"

        try:
            # 1. Search for the company by domain
            search_result = client.crm.companies.search_api.do_search(
                public_object_search_request={
                    "filterGroups": [{
                        "filters": [{"propertyName": FIELD_DOMAIN, "operator": "EQ", "value": domain}]
                    }]
                }
            )

            # 2. Decide: UPDATE or CREATE
            if search_result.results:
                # --- UPDATE EXISTING ---
                company_id = search_result.results[0].id
                client.crm.companies.basic_api.update(
                    company_id, 
                    properties={
                        FIELD_SCORE: score,
                        FIELD_SURGE: is_surge,
                        FIELD_SUMMARY: summary
                    }
                )
                print(f"  [✓] UPDATED: {domain}")
                success_count += 1
            else:
                # --- CREATE NEW (The Fixed Part) ---
                # Step A: Create the dictionary of properties
                new_company_properties = {
                    FIELD_DOMAIN: domain,
                    FIELD_SCORE: intent_score,
                    FIELD_SURGE: is_intent_surge,
                    FIELD_SUMMARY: intent_signal_summary
                }
                
                # Step B: Wrap the dictionary in the required "Envelope" (Model)
                input_obj = SimplePublicObjectInputForCreate(properties=new_company_properties)
                
                # Step C: Pass the envelope to the create method
                client.crm.companies.basic_api.create(simple_public_object_input_for_create=input_obj)
                
                print(f"  [+] CREATED: {domain}")
                created_count += 1

            # Rate limiting protection
            time.sleep(0.1)

        except Exception as e:
            print(f"  [X] Error on {domain}: {e}")
            error_count += 1

    print(f"\n{'='*30}")
    print(f"UPSERT COMPLETE")
    print(f"New Companies Created: {created_count}")
    print(f"Existing Companies Updated: {success_count}")
    print(f"Errors: {error_count}")
    print(f"{'='*30}")

def main():
    client = HubSpot(access_token=HUBSPOT_ACCESS_TOKEN)
    
    try:
        df = pd.read_csv(CSV_FILE_PATH)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    upsert_to_hubspot(client, df)

if __name__ == "__main__":
    main()