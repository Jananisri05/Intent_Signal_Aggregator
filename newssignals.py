import requests
import csv
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from urllib.parse import quote

# ==============================
# HOW THIS WORKS
# Google News RSS is completely free — no API key needed.
# We search for each company + funding keywords together
# so we only get articles actually about that company.
# ==============================

TARGET_COMPANIES = [
    "ClickUp",
    "Airtable",
    "Intercom",
    "Mixpanel",
    "Notion",
    "Lattice",
    "Loom",
    "Benchling",
    "Celonis",
    "Docusign",
    "BambooHR",
    "Segment",
    "Nexthink",
    "Mirakl",
    "Typeform",
    "Productboard",
    "Chargebee",
    "Freshworks",
    "Postman",
    "Motive",
]

# Search queries for each signal type
# Each query is: company name + specific keywords
# This prevents unrelated articles from slipping in
SIGNAL_QUERIES = [
    '{company} raises funding',
    '{company} series funding round',
    '{company} acquires acquisition',
    '{company} new CEO hired',
    '{company} new CRO CMO VP hired',
    '{company} product launch',
    '{company} partnership announced',
    '{company} IPO valuation',
]

# Keywords that must appear in the title for it to count
# If none of these appear, the article is noise
SIGNAL_TITLE_KEYWORDS = [
    "raises", "raised", "funding", "series", "million", "billion",
    "acqui", "hired", "appoints", "launch", "partner", "ipo",
    "valuation", "invest", "round", "growth", "expan"
]

# Words that indicate the article is NOT about the company
# but just mentions the company name
NOISE_KEYWORDS = [
    "intercom mount", "intercom system", "intercom device",
    "notion capital", "segment manager", "segment of",
    "postman job", "loom knitting", "lattice semiconductor",
    "motive partners invests", "motive commercial"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Only articles from last 90 days count as signals
NEWS_WINDOW_DAYS = 90


# ==============================
# CHECK IF ARTICLE DATE IS RECENT
# ==============================
def is_recent(date_str, days=NEWS_WINDOW_DAYS):
    if not date_str:
        return False
    try:
        # RSS dates format: "Thu, 10 Mar 2026 07:00:00 GMT"
        dt = datetime.strptime(date_str[:25].strip(), "%a, %d %b %Y %H:%M:%S")
        cutoff = datetime.now() - timedelta(days=days)
        return dt >= cutoff
    except:
        try:
            dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            cutoff = datetime.now() - timedelta(days=days)
            return dt >= cutoff
        except:
            return False


# ==============================
# CHECK IF TITLE IS A REAL SIGNAL
# ==============================
def is_signal_article(title, company_name):
    title_lower = title.lower()
    company_lower = company_name.lower()

    # Company name must appear in title
    if company_lower not in title_lower:
        return False

    # Check for noise patterns
    for noise in NOISE_KEYWORDS:
        if noise.lower() in title_lower:
            return False

    # At least one signal keyword must appear
    for kw in SIGNAL_TITLE_KEYWORDS:
        if kw.lower() in title_lower:
            return True

    return False


# ==============================
# CLASSIFY WHAT TYPE OF SIGNAL
# ==============================
def classify_signal(title):
    title_lower = title.lower()

    if any(w in title_lower for w in ["raises", "raised", "funding", "series", "round", "million", "billion", "ipo", "valuation"]):
        return "funding"
    elif any(w in title_lower for w in ["acqui", "merger", "buys", "purchased"]):
        return "acquisition"
    elif any(w in title_lower for w in ["hired", "appoints", "names", "joins", "ceo", "cro", "cmo", "cto", "vp"]):
        return "leadership"
    elif any(w in title_lower for w in ["launch", "release", "introduces", "announces product"]):
        return "product"
    elif any(w in title_lower for w in ["partner", "integration", "collaboration"]):
        return "partnership"
    else:
        return "general"


# ==============================
# FETCH GOOGLE NEWS RSS FOR ONE QUERY
# ==============================
def fetch_rss(query):
    encoded_query = quote(query)
    url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        if res.status_code == 200:
            return res.text
        else:
            return None
    except Exception as e:
        return None


# ==============================
# PARSE RSS XML
# ==============================
def parse_rss(xml_text):
    if not xml_text:
        return []

    try:
        root = ET.fromstring(xml_text)
        items = root.findall(".//item")
        articles = []

        for item in items:
            title   = item.findtext("title", "")
            link    = item.findtext("link", "")
            pubdate = item.findtext("pubDate", "")
            source  = item.findtext("source", "")

            articles.append({
                "title":   title,
                "link":    link,
                "pubdate": pubdate,
                "source":  source
            })

        return articles

    except Exception as e:
        return []


# ==============================
# COLLECT NEWS FOR ONE COMPANY
# ==============================
def collect_company_news(company_name):
    found_articles = []
    seen_titles = set()

    for query_template in SIGNAL_QUERIES:
        query = query_template.format(company=company_name)
        xml   = fetch_rss(query)
        articles = parse_rss(xml)

        for article in articles:
            title = article["title"]

            # Skip duplicates
            if title.lower() in seen_titles:
                continue

            # Must be recent
            if not is_recent(article["pubdate"]):
                continue

            # Must be a real signal about this company
            if not is_signal_article(title, company_name):
                continue

            seen_titles.add(title.lower())
            signal_type = classify_signal(title)

            found_articles.append({
                "company":     company_name,
                "title":       title,
                "date":        article["pubdate"][:25].strip() if article["pubdate"] else "N/A",
                "signal_type": signal_type,
                "source":      article["source"],
                "link":        article["link"]
            })

        time.sleep(1)  # Be polite between queries

    return found_articles


# ==============================
# MAIN
# ==============================
def main():
    all_articles    = []
    company_summary = []

    print("=" * 60)
    print("Intent Signal Aggregator — News Collector")
    print(f"Source: Google News RSS (free, no API key)")
    print(f"Companies: {len(TARGET_COMPANIES)}")
    print(f"News window: Last {NEWS_WINDOW_DAYS} days")
    print("=" * 60)

    for i, company in enumerate(TARGET_COMPANIES, 1):
        print(f"\n[{i}/{len(TARGET_COMPANIES)}] {company}")

        articles = collect_company_news(company)

        # Count by signal type
        funding_count    = sum(1 for a in articles if a["signal_type"] == "funding")
        leadership_count = sum(1 for a in articles if a["signal_type"] == "leadership")
        other_count      = len(articles) - funding_count - leadership_count

        print(f"  Articles found: {len(articles)} "
              f"(funding: {funding_count}, "
              f"leadership: {leadership_count}, "
              f"other: {other_count})")

        for a in articles[:3]:  # Show first 3
            print(f"    [{a['signal_type']}] {a['title'][:70]}...")

        all_articles.extend(articles)
        company_summary.append({
            "company":          company,
            "total_articles":   len(articles),
            "funding_articles": funding_count,
            "leadership_articles": leadership_count,
            "other_articles":   other_count,
            "has_news_signal":  len(articles) > 0,
            "status":           "ok" if articles else "no_news"
        })

        time.sleep(2)

    # ==============================
    # SAVE: news_articles.csv
    # ==============================
    with open("news_articles.csv", "w", newline="", encoding="utf-8") as f:
        fieldnames = ["company", "title", "date", "signal_type", "source", "link"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_articles)
    print(f"\nSaved {len(all_articles)} articles → news_articles.csv")

    # ==============================
    # SAVE: news_summary.csv
    # (used by scoring engine in Phase 2)
    # ==============================
    with open("news_summary.csv", "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "company", "total_articles", "funding_articles",
            "leadership_articles", "other_articles",
            "has_news_signal", "status"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(company_summary)
    print(f"Saved summary → news_summary.csv")

    # ==============================
    # PRINT SUMMARY TABLE
    # ==============================
    print("\n" + "=" * 65)
    print("NEWS SUMMARY")
    print("=" * 65)
    print(f"{'Company':<20} {'Total':>6} {'Funding':>8} {'Leadership':>11} {'Signal?':>8}")
    print("-" * 60)

    signal_count = 0
    for r in company_summary:
        has_signal = "YES" if r["has_news_signal"] else "no"
        if r["has_news_signal"]:
            signal_count += 1
        print(
            f"{r['company']:<20}"
            f"{r['total_articles']:>6}"
            f"{r['funding_articles']:>8}"
            f"{r['leadership_articles']:>11}"
            f"{has_signal:>8}"
        )

    print("-" * 60)
    total = sum(r["total_articles"] for r in company_summary)
    print(f"\n{signal_count}/{len(TARGET_COMPANIES)} companies have news signals")
    print(f"Total articles collected: {total}")
    print("Done. Ready for scoring engine.")


if __name__ == "__main__":
    main()