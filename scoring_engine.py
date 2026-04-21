import pandas as pd
import re
from datetime import datetime
from tqdm import tqdm

# ==========================================
# 1. CONFIGURATION & WEIGHTS
# ==========================================

WEIGHTS = {
    'job': 40.0,      
    'funding': 30.0,  
    'news': 30.0      
}

DECAY_WINDOW_DAYS = 90  
SURGE_THRESHOLD_DAYS = 7
SURGE_SIGNAL_COUNT = 3

# Reference date: Set to the most recent date in your news data automatically
REFERENCE_DATE = None 

# ==========================================
# 2. UTILITY FUNCTIONS
# ==========================================

def clean_subdomain(domain):
    if pd.isna(domain) or str(domain).strip() in ('', 'null'):
        return None
    domain = re.sub(r'^(www\.|app\.|blog\.|help\.|support\.|docs\.|api\.)', '', domain.lower().strip())
    parts = domain.split('.')
    return f"{parts[-2]}.{parts[-1]}" if len(parts) > 2 else domain

def parse_strict_date(date_str):
    if pd.isna(date_str) or str(date_str).strip() == '':
        return None
    date_str = str(date_str).strip()

    # Handle Month-Year format (e.g. "Oct 2021")
    if re.fullmatch(r'[A-Za-z]{3}\s\d{4}', date_str):
        try:
            return datetime.strptime(date_str, '%b %Y').replace(day=1)
        except:
            return None

    try:
        dt = pd.to_datetime(date_str, format='mixed', errors='coerce')
        return dt.to_pydatetime() if pd.notna(dt) else None
    except:
        return None

def get_exact_decay_multiplier(signal_date, reference_date):
    if signal_date is None:
        return 0.0
    days_passed = (reference_date - signal_date).days
    if days_passed < 0:
        return 1.0
    return max(0.0, 1.0 - (days_passed / DECAY_WINDOW_DAYS))

# ==========================================
# 3. THE SCORING ENGINE
# ==========================================

def run_scoring_engine(funding_path, jobs_path, news_path):
    print("🚀 Initializing High-Precision Scoring Engine...")

    # Load Data
    df_fund = pd.read_csv(funding_path)
    df_jobs = pd.read_csv(jobs_path)
    df_news = pd.read_csv(news_path)

    # --- FIX: CORRECT COLUMN NAMES FOR JOBS DATASET ---
    # The jobs dataset has 'status' column, no explicit date column
    # We'll use the current date for job signals since no posting date is available
    job_date_col = None
    for col in ['date_posted', 'date', 'posted_date', 'timestamp']:
        if col in df_jobs.columns:
            job_date_col = col
            break
    
    if not job_date_col:
        print(f"⚠️ No date column found in {jobs_path}. Using reference date for all job signals.")
        # We'll handle this by setting parsed_date to reference_date later

    # Parse Dates
    df_fund['last_round_date'] = df_fund['last_round_date'].apply(parse_strict_date)
    df_news['date'] = df_news['date'].apply(parse_strict_date)
    
    if job_date_col:
        df_jobs['parsed_date'] = df_jobs[job_date_col].apply(parse_strict_date)
    else:
        df_jobs['parsed_date'] = None  # Will be filled with reference_date later

    # Set Reference Date
    ref_date_candidate = df_news['date'].max()
    if pd.isna(ref_date_candidate):
        # Fallback if news file is empty/bad
        reference_date = datetime(2026, 4, 15) 
    else:
        reference_date = ref_date_candidate
    print(f"📅 Reference Date (Today): {reference_date.date()}")

    # Clean Domains
    df_fund['canonical_domain'] = df_fund['canonical_domain'].apply(clean_subdomain)
    df_jobs['canonical_domain'] = df_jobs['canonical_domain'].apply(clean_subdomain)
    df_news['canonical_domain'] = df_news['canonical_domain'].apply(clean_subdomain)

    # Build Signal List
    all_signals = []

    # Process Jobs - using 'signal_jobs' column from your dataset
    for _, row in df_jobs.iterrows():
        if pd.notna(row['canonical_domain']):
            n_signals = int(row.get('signal_jobs', 1))  # This column exists in your data
            if pd.notna(row['parsed_date']):
                sig_date = row['parsed_date']
            else:
                sig_date = reference_date  # Use reference date if no job posting date
            for _ in range(n_signals):
                all_signals.append({'domain': row['canonical_domain'], 'type': 'job', 'date': sig_date})

    # Process Funding - using 'last_round_date' and 'last_round_amount' from your dataset
    for _, row in df_fund.iterrows():
        if pd.notna(row['canonical_domain']):
            sig_date = row['last_round_date'] if pd.notna(row['last_round_date']) else datetime(2000,1,1)
            all_signals.append({'domain': row['canonical_domain'], 'type': 'funding', 'date': sig_date})

    # Process News - using 'date' column from your dataset
    for _, row in df_news.iterrows():
        if pd.notna(row['canonical_domain']):
            sig_date = row['date'] if pd.notna(row['date']) else reference_date
            all_signals.append({'domain': row['canonical_domain'], 'type': 'news', 'date': sig_date})

    signals_df = pd.DataFrame(all_signals)
    unique_domains = signals_df['domain'].unique()

    # Scoring
    scored_results = []
    for domain in tqdm(unique_domains, desc="Scoring Accounts"):
        domain_sigs = signals_df[signals_df['domain'] == domain]
        total_score = 0.0
        
        for _, sig in domain_sigs.iterrows():
            weight = WEIGHTS[sig['type']]
            multiplier = get_exact_decay_multiplier(sig['date'], reference_date)
            total_score += (weight * multiplier)

        final_score = min(total_score, 100.0)

        # Surge Detection
        is_surge = False
        sorted_dates = sorted([d for d in domain_sigs['date'].tolist() if d is not None])
        if len(sorted_dates) >= SURGE_SIGNAL_COUNT:
            for i in range(len(sorted_dates) - (SURGE_SIGNAL_COUNT - 1)):
                if (sorted_dates[i + SURGE_SIGNAL_COUNT - 1] - sorted_dates[i]).days <= SURGE_THRESHOLD_DAYS:
                    is_surge = True
                    break

        scored_results.append({
            'domain': domain,
            'score': final_score,
            'is_surge': is_surge,
            'total_signals': len(domain_sigs)
        })

    return pd.DataFrame(scored_results).sort_values('score', ascending=False)

# ==========================================
# 4. EXECUTION
# ==========================================

if __name__ == "__main__":
    # MAKE SURE THESE FILENAMES ARE CORRECT
    FUNDING_CSV = 'normalized_funding.csv'
    JOBS_CSV    = 'normalized_jobs.csv'
    NEWS_CSV    = 'normalized_news.csv'

    try:
        results = run_scoring_engine(FUNDING_CSV, JOBS_CSV, NEWS_CSV)
        
        if results is not None:
            print("\n--- FINAL SCORED ACCOUNTS ---")
            print(results.head(15).to_string(index=False))
            results.to_csv('scored_accounts_precise.csv', index=False)
            print("\n✅ SUCCESS: Results saved to 'scored_accounts_precise.csv'")
        else:
            print("\n❌ ERROR: Engine failed to return results.")

    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")