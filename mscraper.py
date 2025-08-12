# mscraper.py
# Scraper with GPT-powered source guessing for Construction AI tools
# Requirements: pip install requests openai serpapi tldextract

import os, csv, json, time
import requests
import tldextract
from urllib.parse import urlparse
from datetime import datetime
import openai

# --------------- CONFIG ----------------
SERP_API_KEY = os.getenv("SERP_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

openai.api_key = OPENAI_API_KEY

TRUSTED_SOURCES = [
    "Product Hunt", "G2", "An AI For That", "Crunchbase", "AngelList",
    "AppSumo", "LinkedIn", "Reddit", "Medium", "YouTube",
    "BuiltWorlds", "AEC Magazine", "Construction Dive", "Engineering News-Record",
    "BIM+ (Building Information Modelling)", "The B1M", "Construction Executive",
    "Construction Business Owner", "Smart Cities Dive", "InfraTech Digital",
    "ArchDaily", "Dezeen", "DesignBoom", "RICS", "Urban Developer"
]

# --------------- FUNCTIONS ----------------
def serp_search(query, num_results=100):
    """Fetch search results from SerpAPI"""
    print(f"Searching: {query}")
    params = {
        "engine": "google",
        "q": query,
        "num": 10,
        "start": 0,
        "api_key": SERP_API_KEY
    }
    results = []
    for start in range(0, num_results, 10):
        params["start"] = start
        r = requests.get("https://serpapi.com/search", params=params)
        data = r.json()
        organic_results = data.get("organic_results", [])
        for res in organic_results:
            title = res.get("title")
            link = res.get("link")
            snippet = res.get("snippet", "")
            if link:
                results.append({"title": title, "link": link, "snippet": snippet})
        time.sleep(1)  # avoid hitting rate limits
    return results

def guess_source_with_gpt(title, snippet, url):
    """Ask GPT to guess the source from trusted list"""
    domain = tldextract.extract(url).registered_domain
    prompt = f"""
We have a webpage about construction AI tools.
Title: {title}
Snippet: {snippet}
Domain: {domain}

Rules:
1. First check if it matches one of these sources: {TRUSTED_SOURCES}.
2. If not, guess from the title/snippet where it might have come from (e.g., a tech review site, news site, or social media).
3. Domain separation: If the domain itself is the company site (e.g., togal.ai), don't use that as the source. Use a reviewing platform instead.
4. Balance: At least 50% of guessed sources should be from the list above.
5. If no match or reasonable guess, return "General Web".

Return only the guessed source name, nothing else.
"""
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        return resp.choices[0].message["content"].strip()
    except Exception as e:
        print("GPT Guessing Error:", e)
        return "General Web"

def save_to_csv(data, filename="scraped_results.csv"):
    """Save results to CSV"""
    headers = ["Title", "Link", "Snippet", "Source", "Date Scraped"]
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def run_scraper(search_query):
    results = serp_search(search_query)
    final_data = []
    for res in results:
        title = res["title"]
        link = res["link"]
        snippet = res["snippet"]
        source = guess_source_with_gpt(title, snippet, link)
        final_data.append({
            "Title": title,
            "Link": link,
            "Snippet": snippet,
            "Source": source,
            "Date Scraped": datetime.utcnow().isoformat()
        })
    save_to_csv(final_data)
    print(f"Saved {len(final_data)} results to CSV.")

# ----------------- MAIN -----------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python mscraper.py 'search query'")
        sys.exit(1)
    run_scraper(sys.argv[1])