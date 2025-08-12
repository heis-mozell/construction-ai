# mscraper_final.py
# Loosened filtering, fixed CSV headers, reputable source preference, batch size 10
import os, time, json, csv, re, sys
from urllib.parse import urlparse, quote_plus
import requests
import tldextract
import openai
from datetime import datetime

### --------- CONFIG (NO HARDCODED KEYS) ------------
SERP_API_KEY = os.getenv("SERP_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = "gpt-4o-mini"

SERPAPI_URL = "https://serpapi.com/search.json"
PAGES_PER_RUN = 10
RESULTS_PER_PAGE = 10
BATCH_SIZE = 10
OUTPUT_FILE = "construction_tools.csv"
SEEN_FILE = "seen_tools.csv"
LAST_OFFSET_FILE = "last_offset.txt"
RATE_LIMIT_BACKOFF = 8

openai.api_key = OPENAI_API_KEY

REPUTABLE_SOURCES = {
    "producthunt": "Product Hunt",
    "linkedin.com": "LinkedIn",
    "twitter.com": "Twitter",
    "reddit.com": "Reddit",
    "g2.com": "G2",
    "github.com": "GitHub",
    "appsumo.com": "AppSumo",
    "angel.co": "AngelList",
    "crunchbase.com": "Crunchbase",
    "youtube.com": "YouTube",
    "medium.com": "Medium",
    "dev.to": "Dev.to",
    "news": "News",
}

def ensure_output_exists():
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["tool_name", "description", "website", "source", "tags", "reviews", "launch_date"])

def load_seen():
    return set(open(SEEN_FILE).read().splitlines()) if os.path.exists(SEEN_FILE) else set()

def save_seen(seen_set):
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        for s in sorted(seen_set):
            f.write(s + "\n")

def load_last_offset():
    if os.path.exists(LAST_OFFSET_FILE):
        try:
            return int(open(LAST_OFFSET_FILE).read().strip())
        except:
            return 0
    return 0

def save_last_offset(offset):
    open(LAST_OFFSET_FILE, "w").write(str(int(offset)))

def classify_source_and_url(link, displayed_link, snippet, title, tool_name):
    s = (displayed_link or link or "").lower()
    for k,v in REPUTABLE_SOURCES.items():
        if k in s:
            return v, link
    text = (snippet or "") + " " + (title or "")
    if any(k in text.lower() for k in REPUTABLE_SOURCES.keys()):
        for k,v in REPUTABLE_SOURCES.items():
            if k in text.lower():
                return v, link
    return "Google Search", f"https://www.google.com/search?q={quote_plus(tool_name)}"

def clean_json_from_gpt(raw):
    m = re.search(r"(\[.*\])", raw, re.DOTALL)
    return m.group(0) if m else None

def safe_gpt_call(prompt, max_retries=5):
    attempt = 0
    while attempt < max_retries:
        try:
            resp = openai.ChatCompletion.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            return resp["choices"][0]["message"]["content"]
        except Exception as e:
            attempt += 1
            wait = RATE_LIMIT_BACKOFF * attempt
            print(f"⚠️ GPT call failed (attempt {attempt}/{max_retries}): {e}. Backing off {wait}s...")
            time.sleep(wait)
    return None

def build_prompt(batch, batch_num):
    payload = json.dumps(batch, ensure_ascii=False)
    return f"""
You are a data extractor. INPUT is a JSON array of up to {BATCH_SIZE} search results (fields: 'title','snippet','link','source').

Rules:
- If you can find a tool/product name in title or snippet, always include it.
- Always try to fill description, website, source, tags, reviews, launch_date.
- If unknown, use "N/A" (never leave blank).
- website = official domain of the tool (no tracking params).
- source = site name if reputable (Product Hunt, LinkedIn, G2, GitHub, Reddit, AppSumo, AngelList, Crunchbase, YouTube, Medium, Dev.to, News) else "Google Search".
- tags = up to 6 keywords (default: "AI, construction").
- reviews = numeric if found else "0".
- launch_date = month-year or year else "N/A".

Output: STRICT JSON ARRAY ONLY, with objects containing:
tool_name, description, website, source, tags, reviews, launch_date

Batch: {batch_num}
INPUT:
{payload}
"""

def run_serpapi_pages(start_offset, query):
    results = []
    for page in range(PAGES_PER_RUN):
        offset = start_offset + page * RESULTS_PER_PAGE
        try:
            r = requests.get(SERPAPI_URL, params={
                "engine": "google",
                "q": query,
                "start": offset,
                "num": RESULTS_PER_PAGE,
                "api_key": SERP_API_KEY
            }, timeout=20)
            data = r.json()
            for h in data.get("organic_results", []):
                results.append({
                    "title": (h.get("title") or "").strip(),
                    "snippet": (h.get("snippet") or "").strip(),
                    "link": (h.get("link") or h.get("url") or "").strip(),
                    "displayed_link": (h.get("displayed_link") or "").strip()
                })
            time.sleep(0.6)
        except Exception as e:
            print(f"❌ Fetch failed at start={offset}: {e}")
            time.sleep(2)
    return results

if __name__ == "__main__":
    print("=== Construction AI Scraper ===")
    query = input("Search query (default: 'construction AI tools'): ").strip() or "construction AI tools"
    last_offset = load_last_offset()
    mode = input(f"Resume or Start fresh? [R/S] (last_offset={last_offset}): ").strip().upper() or "R"
    start_offset = last_offset if mode == "R" else 0

    ensure_output_exists()
    seen = load_seen()

    raw_results = run_serpapi_pages(start_offset, query)
    unique = []
    seen_keys = set()
    for r in raw_results:
        key = (r.get("link") or "") + "||" + (r.get("title") or "")
        if key not in seen_keys:
            seen_keys.add(key)
            unique.append(r)

    candidates = []
    for item in unique:
        candidates.append({
            "title": item.get("title", ""),
            "snippet": item.get("snippet", ""),
            "link": item.get("link", ""),
            "source": item.get("displayed_link", "")
        })

    total_saved = 0
    for i in range(0, len(candidates), BATCH_SIZE):
        batch = candidates[i:i+BATCH_SIZE]
        prompt = build_prompt(batch, (i // BATCH_SIZE) + 1)
        raw = safe_gpt_call(prompt)
        if not raw: 
            continue
        json_text = clean_json_from_gpt(raw)
        if not json_text:
            continue
        try:
            parsed = json.loads(json_text)
        except:
            continue

        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for obj in parsed:
                tn = obj.get("tool_name", "N/A").strip()
                if tn.lower() in (s.lower() for s in seen):
                    continue
                desc = obj.get("description", "N/A").strip()
                web = obj.get("website", "N/A").strip()
                src, src_url = classify_source_and_url(
                    obj.get("website"), obj.get("source"), desc, tn, tn
                )
                tags = obj.get("tags", "AI, construction").strip()
                reviews = obj.get("reviews", "0").strip()
                launch = obj.get("launch_date", "N/A").strip()

                w.writerow([tn, desc, web, src, tags, reviews, launch])
                seen.add(tn)
                total_saved += 1

    save_last_offset(start_offset + (PAGES_PER_RUN * RESULTS_PER_PAGE))
    save_seen(seen)
    print(f"🎯 Done. {total_saved} new tools saved.")