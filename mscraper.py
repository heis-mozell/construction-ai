# mscraper.py
import os, time, json, csv, re
from urllib.parse import urlparse
import requests
import tldextract
import openai

### -------- CONFIG --------
SERP_API_KEY = os.getenv("SERP_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = "gpt-4o-mini"

SERPAPI_URL = "https://serpapi.com/search.json"
RESULTS_PER_RUN = 100
PAGES_PER_RUN = 10
RESULTS_PER_PAGE = 10
BATCH_SIZE = 10
OUTPUT_FILE = "construction_tools.csv"
SEEN_FILE = "seen_tools.csv"
LAST_OFFSET_FILE = "last_offset.txt"
RATE_LIMIT_BACKOFF = 8

openai.api_key = OPENAI_API_KEY

KNOWN_SOURCES = {
    "producthunt": "Product Hunt",
    "product-hunt": "Product Hunt",
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

# ✅ Correct CSV header (7 columns)
def ensure_output_exists():
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "tool_name",
                "description",
                "website",
                "source",
                "tags",
                "reviews",
                "launch_date"
            ])

def load_seen():
    if os.path.exists(SEEN_FILE):
        return set(open(SEEN_FILE, "r", encoding="utf-8").read().splitlines())
    return set()

def save_seen(seen_set):
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        for s in sorted(seen_set):
            f.write(s + "\n")

def load_last_offset():
    try:
        return int(open(LAST_OFFSET_FILE, "r").read().strip())
    except:
        return 0

def save_last_offset(offset):
    with open(LAST_OFFSET_FILE, "w", encoding="utf-8") as f:
        f.write(str(int(offset)))

def classify_source(displayed_link, snippet, title):
    s = (displayed_link or "").lower()
    for k, v in KNOWN_SOURCES.items():
        if k in s:
            return v
    txt = (snippet or "") + " " + (title or "")
    txt = txt.lower()
    if "product hunt" in txt:
        return "Product Hunt"
    if "linkedin" in txt:
        return "LinkedIn"
    if "reddit" in txt:
        return "Reddit"
    if "g2" in txt:
        return "G2"
    return "Google Search"

def extract_review_count(text):
    if not text:
        return "0"
    m = re.search(r"(\d{1,3}(?:[,\s]\d{3})*)\s+(?:reviews?|ratings?|votes?)", text, re.I)
    if m:
        return re.sub(r"[,\s]", "", m.group(1))
    m2 = re.search(r"(\d{1,3}(?:[,\s]\d{3})*)\s+(?:users|customers|clients)", text, re.I)
    if m2:
        return re.sub(r"[,\s]", "", m2.group(1))
    return "0"

def clean_json_from_gpt(raw):
    if not raw:
        return None
    m = re.search(r"(\[.*\])", raw, re.DOTALL)
    return m.group(0) if m else None

def safe_gpt_call(prompt, max_retries=5):
    attempt = 0
    while attempt < max_retries:
        try:
            resp = openai.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            return resp.choices[0].message.content
        except Exception as e:
            attempt += 1
            wait = RATE_LIMIT_BACKOFF * attempt
            print(f"⚠️ GPT call failed ({attempt}/{max_retries}): {e} → retrying in {wait}s")
            time.sleep(wait)
    return None

def build_prompt(batch, batch_num):
    payload = json.dumps(batch, ensure_ascii=False)
    return f"""
Extract tool details if tool_name is found in title or snippet.
Return STRICT JSON array of objects with:
tool_name, description (8-30 words), website (domain only),
source (prefer reputable site from list, else "Google Search"),
tags (max 6), reviews (integer string), launch_date.
Skip entries without tool_name.
Batch: {batch_num}
INPUT:
{payload}
"""

def run_serpapi_pages(start_offset):
    all_results = []
    for page in range(PAGES_PER_RUN):
        offset = start_offset + page * RESULTS_PER_PAGE
        params = {
            "engine": "google",
            "q": QUERY,
            "start": offset,
            "num": RESULTS_PER_PAGE,
            "api_key": SERP_API_KEY
        }
        try:
            r = requests.get(SERPAPI_URL, params=params, timeout=20)
            r.raise_for_status()
            hits = r.json().get("organic_results") or []
            for h in hits:
                all_results.append({
                    "title": h.get("title") or "",
                    "snippet": h.get("snippet") or "",
                    "link": h.get("link") or h.get("url") or "",
                    "displayed_link": h.get("displayed_link") or ""
                })
            time.sleep(0.6)
        except Exception as e:
            print(f"❌ SERP fetch failed: {e}")
            time.sleep(2)
    return all_results