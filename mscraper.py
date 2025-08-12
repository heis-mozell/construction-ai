# mscraper_final.py
# Final scraper: 10 pages (≈100 results) per run, resume support, strict GPT extraction + dedupe.
# Requirements: pip install requests openai tldextract

import os, time, json, csv, re, sys
from urllib.parse import urlparse
import requests
import tldextract
from openai import OpenAI  # ✅ Updated for OpenAI >= 1.0.0
from datetime import datetime

### --------- CONFIG ------------
SERP_API_KEY = os.getenv("SERP_API_KEY", "YOUR_SERP_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "YOUR_OPENAI_KEY")
OPENAI_MODEL = "gpt-4o-mini"

SERPAPI_URL = "https://serpapi.com/search.json"
RESULTS_PER_RUN = 100
PAGES_PER_RUN = 10
RESULTS_PER_PAGE = 10
BATCH_SIZE = 10  # ✅ Reduced batch size for better accuracy
OUTPUT_FILE = "construction_tools.csv"
SEEN_FILE = "seen_tools.csv"
LAST_OFFSET_FILE = "last_offset.txt"
RATE_LIMIT_BACKOFF = 8

# ✅ Initialize OpenAI client (new API)
client = OpenAI(api_key=OPENAI_API_KEY)

# Known source domain keywords mapping
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

# ---------- Helpers ----------
def ensure_output_exists():
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["tool_name", "description", "website", "source", "tags", "reviews", "launch_date", "scrape_date"])

def load_seen():
    s = set()
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            for line in f:
                v = line.strip()
                if v:
                    s.add(v)
    return s

def save_seen(seen_set):
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        for s in sorted(seen_set):
            f.write(s + "\n")

def load_last_offset():
    if os.path.exists(LAST_OFFSET_FILE):
        try:
            return int(open(LAST_OFFSET_FILE, "r").read().strip())
        except:
            return 0
    return 0

def save_last_offset(offset):
    with open(LAST_OFFSET_FILE, "w", encoding="utf-8") as f:
        f.write(str(int(offset)))

def domain_from_url(url):
    if not url:
        return ""
    try:
        ex = tldextract.extract(url)
        if ex.domain:
            return (ex.domain + (("." + ex.suffix) if ex.suffix else "")).lower()
    except:
        pass
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except:
        return ""

def classify_source(displayed_link, snippet, title):
    s = (displayed_link or "").lower()
    for k,v in KNOWN_SOURCES.items():
        if k in s:
            return v
    txt = (snippet or "") + " " + (title or "")
    txt = txt.lower()
    if "product hunt" in txt or "producthunt" in txt:
        return "Product Hunt"
    if "linkedin" in txt or "company on linkedin" in txt:
        return "LinkedIn"
    if "reddit" in txt or "r/" in txt:
        return "Reddit"
    if "g2" in txt:
        return "G2"
    if any(word in txt for word in ["newsletter", "subscribe", "issue #", "issue:"]):
        return "Newsletter"
    if any(word in txt for word in ["blog", "post", "article"]):
        return "Blog"
    if any(word in txt for word in ["tweet", "x.com", "twitter", "threads"]):
        return "Social Media"
    if s:
        return s
    return "blog"

def extract_review_count(text):
    if not text:
        return "0"
    m = re.search(r"(\d{1,3}(?:[,\s]\d{3})*)\s+(?:reviews?|ratings?|votes?)", text, re.IGNORECASE)
    if m:
        return re.sub(r"[,\s]", "", m.group(1))
    m2 = re.search(r"(\d{1,3}(?:[,\s]\d{3})*)\s+(?:users|customers|clients)", text, re.IGNORECASE)
    if m2:
        return re.sub(r"[,\s]", "", m2.group(1))
    return "0"

def clean_json_from_gpt(raw):
    if not raw:
        return None
    m = re.search(r"(\[.*\]|\{.*\})", raw, re.DOTALL)
    return m.group(0) if m else None

# ✅ Updated for new API
def safe_gpt_call(prompt, max_retries=5):
    attempt = 0
    while attempt < max_retries:
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            text = resp.choices[0].message.content
            return text
        except Exception as e:
            attempt += 1
            wait = RATE_LIMIT_BACKOFF * attempt
            print(f"⚠️ GPT call failed (attempt {attempt}/{max_retries}): {e}. Backing off {wait}s...")
            time.sleep(wait)
    return None

# Build prompt
def build_prompt(batch, batch_num):
    payload = json.dumps(batch, ensure_ascii=False)
    prompt = f"""
You are a strict extractor. INPUT is a JSON array of up to {BATCH_SIZE} search results.
For each item, if tool_name is found in title or snippet, INCLUDE it. 
If missing other fields, try to deduce logically from title/snippet but do not leave empty strings.

Output: JSON ARRAY ONLY. Fields:
- tool_name
- description
- website (domain only)
- source (prefer known platforms like LinkedIn, Product Hunt, etc., else Google Search query link)
- tags
- reviews
- launch_date
Batch {batch_num}
INPUT:
{payload}
"""
    return prompt

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
            data = r.json()
            hits = data.get("organic_results") or []
            for h in hits:
                all_results.append({
                    "title": (h.get("title") or "").strip(),
                    "snippet": (h.get("snippet") or "").strip(),
                    "link": (h.get("link") or h.get("url") or "").strip(),
                    "displayed_link": (h.get("displayed_link") or h.get("source") or "").strip()
                })
            time.sleep(0.6)
        except Exception as e:
            print(f"❌ SerpAPI page fetch failed at start={offset}: {e}")
            time.sleep(2)
            continue
    return all_results

# ---------- Main ----------
if __name__ == "__main__":
    print("=== Construction AI scraper ===")
    QUERY = input("Search query (default: 'construction AI tools'): ").strip() or "construction AI tools"

    last_offset = load_last_offset()
    mode = ""
    while mode not in ("R", "S"):
        mode = input(f"Resume or Start fresh? [R/S] (last_offset={last_offset}): ").strip().upper() or "R"
    start_offset = last_offset if mode == "R" else 0

    ensure_output_exists()
    seen = load_seen()

    print(f"🔍 Fetching results starting at offset {start_offset}...")
    raw_results = run_serpapi_pages(start_offset)
    print(f"⚙️ Collected {len(raw_results)} raw candidates.")

    unique, seen_keys = [], set()
    for r in raw_results:
        key = (r.get("link") or "") + "||" + (r.get("title") or "")
        if key not in seen_keys:
            seen_keys.add(key)
            unique.append(r)

    candidates = []
    for item in unique:
        src = classify_source(item.get("displayed_link",""), item.get("snippet",""), item.get("title",""))
        candidates.append({
            "title": item.get("title",""),
            "snippet": item.get("snippet",""),
            "link": item.get("link",""),
            "source": src
        })

    filtered = []
    for c in candidates:
        naive_name = (c["title"] or "").split("—")[0].split("|")[0].strip()
        if naive_name and naive_name.lower() in (s.lower() for s in seen):
            continue
        filtered.append(c)

    total_saved = 0
    batch_num = 0
    for i in range(0, len(filtered), BATCH_SIZE):
        batch = filtered[i:i+BATCH_SIZE]
        batch_num += 1
        print(f"⚙️ Sending batch {batch_num} to GPT...")
        raw = safe_gpt_call(build_prompt(batch, batch_num))
        if not raw: continue
        json_text = clean_json_from_gpt(raw)
        if not json_text: continue
        try:
            parsed = json.loads(json_text)
            if not isinstance(parsed, list): continue
        except: continue

        written = 0
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for obj in parsed:
                tn = (obj.get("tool_name") or "").strip()
                if not tn: continue
                desc = (obj.get("description") or "").strip()
                web = (obj.get("website") or "").strip()
                src = (obj.get("source") or "").strip()
                tags = (obj.get("tags") or "AI, construction").strip()
                reviews = (obj.get("reviews") or "0").strip()
                launch = (obj.get("launch_date") or "").strip()

                if tn.lower() in (s.lower() for s in seen): continue
                w.writerow([tn, desc, web, src, tags, reviews, launch, datetime.utcnow().isoformat()])
                seen.add(tn)
                written += 1
                total_saved += 1
        print(f"✅ Batch {batch_num} -> saved {written} new tools.")
        time.sleep(1.2)

    save_last_offset(start_offset + (PAGES_PER_RUN * RESULTS_PER_PAGE))
    save_seen(seen)
    print(f"🎯 Done. Total new tools saved: {total_saved}")