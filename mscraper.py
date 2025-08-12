# mscraper.py
# Final scraper: dynamic query input, GPT source guessing, CSV export
# Requirements: pip install requests openai tldextract python-dotenv

import os
import time
import csv
import re
import requests
import tldextract
import openai
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SERP_API_KEY = os.getenv("SERP_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

# Trusted source list for matching
TRUSTED_SOURCES = [
    "Product Hunt", "G2", "An AI For That", "Crunchbase", "AngelList", "AppSumo", "LinkedIn",
    "Reddit", "Medium", "YouTube", "BuiltWorlds", "ForConstructionPros", "AEC Magazine",
    "Construction Dive", "ENR", "ArchDaily", "BIM+", "Smart Cities Dive", "Fast Company"
]

# --- GPT Source Guess ---
def guess_source(title, snippet, domain):
    try:
        prompt = f"""
        You are given a title, snippet, and domain of a tool.
        Match it to the most likely source/platform from this list:
        {', '.join(TRUSTED_SOURCES)}.

        Rules:
        - If domain exactly matches the source's domain, pick that source.
        - If snippet/title contains brand clues, pick that source.
        - If no clear match, guess from the list — avoid "General Web" unless absolutely no clue.
        - Domain for source must be different from the tool's domain.

        Title: {title}
        Snippet: {snippet}
        Domain: {domain}

        Respond with ONLY the source name.
        """

        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=10,
            temperature=0
        )
        return response.choices[0].message["content"].strip()
    except Exception:
        return "General Web"

# --- SERP API Search ---
def serpapi_search(query, page):
    url = "https://serpapi.com/search.json"
    params = {
        "engine": "google",
        "q": query,
        "start": (page - 1) * 10,
        "api_key": SERP_API_KEY
    }
    r = requests.get(url, params=params)
    if r.status_code != 200:
        return []
    return r.json().get("organic_results", [])

# --- Main Scraper Function ---
def run_serpapi_pages(query, pages=10):
    if not query:
        print("No query provided.")
        return []

    all_results = []
    seen_links = set()

    for page in range(1, pages + 1):
        print(f"Scraping page {page} for query: {query}...")
        results = serpapi_search(query, page)

        for res in results:
            link = res.get("link")
            if not link or link in seen_links:
                continue

            seen_links.add(link)
            title = res.get("title", "")
            snippet = res.get("snippet", "")
            domain = tldextract.extract(link).registered_domain

            source = guess_source(title, snippet, domain)

            all_results.append({
                "Title": title,
                "Link": link,
                "Snippet": snippet,
                "Domain": domain,
                "Source": source
            })

        time.sleep(1)  # Avoid hitting API too fast

    # Save CSV
    filename = f"scraper_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Title", "Link", "Snippet", "Domain", "Source"])
        writer.writeheader()
        writer.writerows(all_results)

    print(f"Saved {len(all_results)} results to {filename}")
    return all_results