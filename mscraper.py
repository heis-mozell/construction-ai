# mscraper.py
# Construction AI tool scraper with GPT-assisted source guessing
# Requirements: pip install requests openai tldextract

import os
import requests
import json
import csv
import tldextract
from datetime import datetime
from urllib.parse import urlparse
import openai

# Load API keys from environment
SERP_API_KEY = os.getenv("SERP_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

# ------------------ Trusted Sources List ------------------
TRUSTED_SOURCES = [
    "producthunt.com", "g2.com", "an.ai.for.that", "crunchbase.com",
    "angellist.com", "appsumo.com", "linkedin.com", "reddit.com",
    "medium.com", "youtube.com", "builtworlds.com", "constructionexec.com",
    "aecbusiness.com", "constructconnect.com", "forconstructionpros.com",
    "engineeringnewsrecord.com", "archdaily.com", "constructionspecifier.com",
    "constructiondive.com", "globalconstructionreview.com",
    "civilplus.com", "archinect.com", "houzz.com", "designboom.com",
    "venturebeat.com"
]

# ------------------ Helper: Classify Source ------------------
def classify_source(url, title="", snippet=""):
    """
    Classifies the source of a tool based on rules:
    1. Primary match: Trusted source list
    2. Secondary: Detect brand clue in title/snippet
    3. GPT-assisted guess
    4. Fallback: 'General Web'
    """
    domain_info = tldextract.extract(url)
    domain = f"{domain_info.domain}.{domain_info.suffix}".lower()

    # Rule 1: Direct match in trusted sources (different from tool domain)
    for src in TRUSTED_SOURCES:
        if src != domain and src in domain:
            return src

    # Rule 2: Brand clue in title/snippet
    text_to_check = f"{title} {snippet}".lower()
    for src in TRUSTED_SOURCES:
        if src.replace("www.", "").split(".")[0] in text_to_check:
            return src

    # Rule 3: GPT-assisted guess
    try:
        prompt = f"""Given the following tool info, guess the most likely platform or site it came from.
Title: {title}
Snippet: {snippet}
URL: {url}

Pick from: {', '.join(TRUSTED_SOURCES)}.
If none fit, choose the most likely instead of 'General Web'.
Answer with only the domain."""
        
        resp = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=10,
            temperature=0
        )
        guess = resp.choices[0].message["content"].strip().lower()
        if guess and guess != "general web":
            return guess
    except Exception:
        pass

    # Rule 4: Fallback
    return "General Web"

# ------------------ Main Scraper ------------------
def scrape_and_save(query, output_csv="results.csv", num_pages=5):
    all_results = []

    for page in range(num_pages):
        start = page * 10
        url = f"https://serpapi.com/search.json?q={query}&engine=google&start={start}&api_key={SERP_API_KEY}"
        resp = requests.get(url)
        data = resp.json()

        if "organic_results" not in data:
            continue

        for res in data["organic_results"]:
            link = res.get("link", "")
            title = res.get("title", "")
            snippet = res.get("snippet", "")
            source = classify_source(link, title, snippet)

            all_results.append({
                "Title": title,
                "Link": link,
                "Snippet": snippet,
                "Source": source,
                "Date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            })

    # Save to CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Title", "Link", "Snippet", "Source", "Date"])
        writer.writeheader()
        writer.writerows(all_results)

    return all_results

if __name__ == "__main__":
    q = input("Enter search query: ")
    scrape_and_save(q)