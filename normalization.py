import pandas as pd
import requests
import re
import time
import os
from tqdm import tqdm

# ==========================================
# 1. CONFIGURATION (PASTE YOUR NEW KEYS HERE)
# ==========================================
# IMPORTANT: Once you regenerate your keys, paste them below.
SERPAPI_KEY = "4268be3385210c1ed5f3a168b41373f7ee0588bd65ce4d2357cbbe32350ebd67" 
HUNTER_API_KEY = "430a4bdb179037882ba0f91eb2434da49a4bb7d4"

# Set this to False when you are ready to use your real CSV files
MOCK_MODE = False

# ==========================================
# 2. CORE FUNCTIONS
# ==========================================

def get_domain_from_google(company_name):
    """Uses SerpApi to search Google and find the most likely official website."""
    if MOCK_MODE:
        # Simulating a search result for testing
        mock_map = {"ClickUp": "clickup.com", "Airtable": "airtable.com", "Mixpanel": "mixpanel.com", "Loom": "loom.com"}
        return mock_map.get(company_name, None)

    query = f"official website of {company_name}"
    params = {
        "q": query,
        "api_key": SERPAPI_KEY,
        "engine": "google"
    }

    try:
        response = requests.get("https://serpapi.com/search", params=params, timeout=10)
        if response.status_code == 200:
            results = response.json()
            first_result = results.get("organic_results", [])[0].get("link")
            if first_result:
                # Regex to extract domain: https://www.acme.com/page -> acme.com
                domain_match = re.search(r'https?://(?:www\.)?([^/]+)', first_result)
                if domain_match:
                    return domain_match.group(1)
        return None
    except Exception as e:
        print(f"❌ Search Error for {company_name}: {e}")
        return None

def verify_domain_with_hunter(domain):
    """Uses Hunter.io to confirm the domain is a valid business domain."""
    if not domain or MOCK_MODE:
        return True # Skip verification in Mock Mode

    url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={HUNTER_API_KEY}"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            # If Hunter finds data, the domain is valid
            return data.get("data") is not None
        return False
    except Exception as e:
        print(f"❌ Hunter Error for {domain}: {e}")
        return False

# ==========================================
# 3. MAIN PIPELINE
# ==========================================

def run_hybrid_normalization(dataframes):
    print("🚀 Starting Phase 2: Hybrid Normalization (Search + Hunter.io)...")

    # 1. Extract all unique company names across all datasets
    all_names = []
    for df in dataframes:
        all_names.extend(df['company'].unique().tolist())
    unique_names = list(set(all_names))

    print(f"🔍 Found {len(unique_names)} unique company names. Starting enrichment...")

    # 2. Process names
    master_domain_map = {}
    
    for name in tqdm(unique_names, desc="Mapping Names to Domains"):
        # STEP 1: Search Google for the domain
        found_domain = get_domain_from_google(name)
        
        if found_domain:
            # STEP 2: Verify with Hunter.io
            if verify_domain_with_hunter(found_domain):
                master_domain_map[name] = found_domain
            else:
                master_domain_map[name] = None 
        else:
            master_domain_map[name] = None

        # Respect API rate limits
        time.sleep(0.2)

    print(f"✅ Enrichment complete. Mapped {len(master_domain_map)} companies.")

    # 3. Apply mapping back to all original DataFrames
    for df in dataframes:
        df['canonical_domain'] = df['company'].map(master_domain_map)
        # Requirement 11: Flag for manual review
        df['needs_manual_review'] = df['canonical_domain'].isna()

    print("✨ All datasets normalized.")
    return dataframes

# ==========================================
# 4. EXECUTION (RUNNING THE PROGRAM)
# ==========================================

if __name__ == "__main__":
    
    if MOCK_MODE:
        print("⚠️ RUNNING IN MOCK MODE (Using dummy data)")
        df_funding = pd.DataFrame({'company': ['ClickUp', 'Airtable', 'FakeCompanyXYZ']})
        df_jobs = pd.DataFrame({'company': ['ClickUp', 'Mixpanel']})
        df_news = pd.DataFrame({'company': ['Airtable', 'Loom']})
    else:
        print("📂 LOADING REAL CSV FILES...")
        try:
            # CHANGE THESE TO YOUR ACTUAL FILE NAMES
            df_funding = pd.read_csv('company_funding_complete.csv')
            df_jobs = pd.read_csv('company_summary.csv')
            df_news = pd.read_csv('news_articles.csv')
        except FileNotFoundError as e:
            print(f"❌ Error: Could not find your CSV files. Make sure they are in the same folder. {e}")
            exit()

    # Execute the pipeline
    normalized_dfs = run_hybrid_normalization([df_funding, df_jobs, df_news])

    # Unpack the list of DataFrames
    clean_funding, clean_jobs, clean_news = normalized_dfs

    # 5. SAVE THE OUTPUTS TO YOUR COMPUTER
    print("\n💾 Saving files to your project folder...")
    clean_funding.to_csv('normalized_funding.csv', index=False)
    clean_jobs.to_csv('normalized_jobs.csv', index=False)
    clean_news.to_csv('normalized_news.csv', index=False)

    print("✅ SUCCESS: All files are saved and ready for Phase 3!")
    print("- normalized_funding.csv")
    print("- normalized_jobs.csv")
    print("- normalized_news.csv")