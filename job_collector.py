import pandas as pd
import requests
import time

INPUT_FILE = "news_summary.csv"
OUTPUT_FILE = "job_postings_output.csv"

# 🔐 Your RapidAPI Key (embedded as requested)
RAPIDAPI_KEY = "b264da2e00msh09f307f1f7b0ce1p14a745jsn94b1d157891d"


# -----------------------------
# JSEARCH (RAPIDAPI)
# -----------------------------
def fetch_jsearch_jobs(company):
    jobs = []
    try:
        url = "https://jsearch.p.rapidapi.com/search"

        querystring = {
            "query": f"software engineer at {company}",
            "page": "1",
            "num_pages": "1"
        }

        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring)

        if response.status_code != 200:
            print(f"JSearch API failed: {response.status_code}")
            return jobs

        data = response.json()

        for job in data.get("data", []):
            jobs.append({
                "company": company,
                "title": job.get("job_title"),
                "location": job.get("job_city"),
                "url": job.get("job_apply_link"),
                "source": "JSearch",
                "hiring_signal": "high"
            })

    except Exception as e:
        print(f"JSearch error for {company}: {e}")

    return jobs


# -----------------------------
# GREENHOUSE
# -----------------------------
def fetch_greenhouse_jobs(company):
    jobs = []
    try:
        url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
        response = requests.get(url)

        if response.status_code != 200:
            return jobs

        data = response.json()

        for job in data.get("jobs", []):
            jobs.append({
                "company": company,
                "title": job.get("title"),
                "location": job.get("location", {}).get("name"),
                "url": job.get("absolute_url"),
                "source": "Greenhouse",
                "hiring_signal": "medium"
            })

    except:
        pass

    return jobs


# -----------------------------
# LEVER
# -----------------------------
def fetch_lever_jobs(company):
    jobs = []
    try:
        url = f"https://api.lever.co/v0/postings/{company}?mode=json"
        response = requests.get(url)

        if response.status_code != 200:
            return jobs

        data = response.json()

        for job in data:
            jobs.append({
                "company": company,
                "title": job.get("text"),
                "location": job.get("categories", {}).get("location"),
                "url": job.get("hostedUrl"),
                "source": "Lever",
                "hiring_signal": "medium"
            })

    except:
        pass

    return jobs


# -----------------------------
# FALLBACK
# -----------------------------
def fallback_jobs(company):
    query = company.replace(" ", "+") + "+software+engineer+jobs"

    return [{
        "company": company,
        "title": "Search Jobs",
        "location": "N/A",
        "url": f"https://www.google.com/search?q={query}",
        "source": "Fallback",
        "hiring_signal": "low"
    }]


# -----------------------------
# NORMALIZE COMPANY NAME
# -----------------------------
def normalize_company(company):
    return company.lower().replace(" ", "").replace(",", "")


# -----------------------------
# MAIN
# -----------------------------
def main():
    df = pd.read_csv(INPUT_FILE)

    company_col = None
    for col in ["company", "Company", "company_name", "Company Name"]:
        if col in df.columns:
            company_col = col
            break

    if not company_col:
        raise Exception("Company column not found")

    all_jobs = []

    for company in df[company_col].dropna().unique():
        print(f"\nProcessing: {company}")

        # 1️⃣ JSEARCH
        jobs = fetch_jsearch_jobs(company)

        # 2️⃣ GREENHOUSE
        if not jobs:
            print("→ JSearch failed, trying Greenhouse...")
            jobs = fetch_greenhouse_jobs(normalize_company(company))

        # 3️⃣ LEVER
        if not jobs:
            print("→ Greenhouse failed, trying Lever...")
            jobs = fetch_lever_jobs(normalize_company(company))

        # 4️⃣ FALLBACK
        if not jobs:
            print("→ All failed, using fallback...")
            jobs = fallback_jobs(company)

        all_jobs.extend(jobs)
        time.sleep(1)

    pd.DataFrame(all_jobs).to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()