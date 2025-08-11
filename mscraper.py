# mscraper_final.py
# Final scraper: 10 pages (≈100 results) per run, resume support, strict GPT extraction + dedupe.
# Requirements: pip install requests openai tldextract

import os, time, json, csv, re, sys
from urllib.parse import urlparse
import requests
import tldextract
from datetime import datetime
from dotenv import load_dotenv

# Load .env file
load_dotenv()

### --------- CONFIG (keys from .env) ------------
import openai

openai.api_key = OPENAI_API_KEY
SERP_API_KEY = os.getenv("SERP_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = "gpt-4o-mini"   # uses Chat Completions interface
SERPAPI_URL = "https://serpapi.com/search.json"
RESULTS_PER_RUN = 100           # 10 pages * 10 results/page
PAGES_PER_RUN = 10
RESULTS_PER_PAGE = 10
BATCH_SIZE = 20                 # how many candidates per GPT call
OUTPUT_FILE = "construction_tools.csv"
SEEN_FILE = "seen_tools.csv"
LAST_OFFSET_FILE = "last_offset.txt"
RATE_LIMIT_BACKOFF = 8          # base seconds for GPT retries

# initialize openai
openai.api_key = OPENAI_API_KEY

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

# helpers
def ensure_output_exists():
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["tool_name","description","website","source","tags","reviews","launch_date","scrape_date"])

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
    # direct mapping from domain
    for k,v in KNOWN_SOURCES.items():
        if k in s:
            return v
    # fallback checks in snippet/title text
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
    # final fallback: the domain itself (site) if non-empty else "blog"
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

def safe_gpt_call(prompt, max_retries=5):
    attempt = 0
    while attempt < max_retries:
        try:
            resp = openai.ChatCompletion.create(
                model=OPENAI_MODEL,
                messages=[{"role":"user","content": prompt}],
                temperature=0
            )
            text = resp["choices"][0]["message"]["content"]
            return text
        except Exception as e:
            attempt += 1
            wait = RATE_LIMIT_BACKOFF * attempt
            print(f"⚠️ GPT call failed (attempt {attempt}/{max_retries}): {e}. Backing off {wait}s...")
            time.sleep(wait)
    return None

# Build strict prompt for a batch
def build_prompt(batch, batch_num):
    # batch is list of dicts: title, snippet, link, source
    payload = json.dumps(batch, ensure_ascii=False)
    prompt = f"""
You are a strict extractor. INPUT is a JSON array of up to {BATCH_SIZE} search results (each contains 'title','snippet','link','source').
For each input item, extract ONLY if you can confidently find a canonical product/tool NAME in the title or snippet.
Do NOT invent or guess tool names. Use only the provided text.

Output: JSON ARRAY ONLY (no commentary, no markdown). Each array item is an object with:
- tool_name (string)  -- MUST be present for the object to be included
- description (short string, 8-30 words) -- prefer snippet trimmed to a concise sentence, no repetition
- website (domain only, e.g. 'fleetcommand.io') -- derive from 'link'
- source (string) -- use the provided 'source' value (do not convert domain to something else)
- tags (comma-separated keywords from title or snippet, max 6). If none found, use "AI, construction"
- reviews (integer string) -- numeric count if explicit in snippet, else "0"
- launch_date (short month-year or year if present) -- empty string if not present

Rules:
- STRICT JSON ONLY: return a JSON ARRAY of objects, nothing else.
- If tool_name cannot be determined confidently from the given fields, skip that item.
- Do not add extra fields.
- Use only the input fields (title, snippet, link, source) to extract data.

Batch number: {batch_num}
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
            # fallback: some SERP entries might be in "top_results" or "knowledge_graph"; include only organic
            for h in hits:
                title = (h.get("title") or "").strip()
                snippet = (h.get("snippet") or "").strip()
                link = (h.get("link") or h.get("url") or "").strip()
                displayed_link = (h.get("displayed_link") or h.get("source") or "").strip()
                all_results.append({
                    "title": title,
                    "snippet": snippet,
                    "link": link,
                    "displayed_link": displayed_link
                })
            # polite delay between page requests
            time.sleep(0.6)
        except Exception as e:
            print(f"❌ SerpAPI page fetch failed at start={offset}: {e}")
            # try to continue next page
            time.sleep(2)
            continue
    return all_results

# ---------------- Main flow ----------------
if __name__ == "__main__":
    print("=== Construction AI scraper (10 pages / run) ===")
    # get query prompt from user or default
    QUERY = input("Search query (default: 'construction AI tools'): ").strip() or "construction AI tools"

    # choose resume or start fresh
    last_offset = load_last_offset()
    mode = ""
    while mode not in ("R", "S"):
        mode = input(f"Resume or Start fresh? [R/S] (last_offset={last_offset}): ").strip().upper() or "R"
    if mode == "R":
        start_offset = last_offset
    else:
        start_offset = 0

    ensure_output_exists()
    seen = load_seen()

    print(f"🔍 Fetching up to {RESULTS_PER_RUN} results (pages {PAGES_PER_RUN}) starting at offset {start_offset}...")
    raw_results = run_serpapi_pages(start_offset)
    print(f"⚙️ Collected {len(raw_results)} raw SERP candidates (dedup & preprocess next).")

    # dedupe by link+title quickly
    unique = []
    seen_keys = set()
    for r in raw_results:
        key = (r.get("link") or "") + "||" + (r.get("title") or "")
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique.append(r)
    print(f"⚙️ {len(unique)} unique candidates after initial dedupe.")

    # prepare candidates for GPT: include source classification
    candidates = []
    for item in unique:
        title = item.get("title","")
        snippet = item.get("snippet","")
        link = item.get("link","")
        displayed_link = item.get("displayed_link","")
        src = classify_source(displayed_link, snippet, title)
        candidates.append({
            "title": title,
            "snippet": snippet,
            "link": link,
            "source": src
        })

    # remove ones already seen by tool_name if we can detect a tool_name naive from title (quick filter)
    # won't be authoritative — final dedupe happens after GPT extraction using seen_tool names.
    filtered = []
    for c in candidates:
        naive_name = (c["title"] or "").split("—")[0].split("|")[0].strip()
        if naive_name and naive_name.lower() in (s.lower() for s in seen):
            continue
        filtered.append(c)
    print(f"⚙️ {len(filtered)} candidates passed naive seen check (final dedupe after GPT).")

    # Process in batches via GPT
    total_saved = 0
    batch_num = 0
    for i in range(0, len(filtered), BATCH_SIZE):
        batch = filtered[i:i+BATCH_SIZE]
        batch_num += 1
        print(f"⚙️ Sending batch {batch_num} ({i+1}-{i+len(batch)}) to GPT...")
        prompt = build_prompt(batch, batch_num)
        raw = safe_gpt_call(prompt, max_retries=5)
        if not raw:
            print("⚠️ No GPT output for this batch — skipping.")
            continue
        json_text = clean_json_from_gpt(raw)
        if not json_text:
            print("⚠️ GPT did not return valid JSON block — skipping batch.")
            continue
        try:
            parsed = json.loads(json_text)
            if not isinstance(parsed, list):
                print("⚠️ GPT JSON not an array — skipping batch.")
                continue
        except Exception as e:
            print("⚠️ Failed to parse GPT JSON:", e)
            continue

        # write validated rows
        written = 0
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for obj in parsed:
                tn = (obj.get("tool_name") or "").strip()
                desc = (obj.get("description") or "").strip()
                web = (obj.get("website") or "").strip()
                src = (obj.get("source") or "").strip()
                tags = (obj.get("tags") or "").strip()
                reviews = (obj.get("reviews") or "").strip()
                launch = (obj.get("launch_date") or "").strip()
                if not tn or not desc or not web or not src:
                    # enforce mandatory fields; skip weak records
                    continue
                # normalize reviews to digits
                if not re.match(r"^\d+$", reviews or ""):
                    reviews = extract_review_count(desc + " " + (batch[0].get("snippet","") if batch else ""))
                    if not reviews:
                        reviews = "0"
                if not tags:
                    tags = "AI, construction"
                # dedupe by tool_name
                if tn.lower() in (s.lower() for s in seen):
                    continue
                w.writerow([tn, desc, web, src, tags, reviews, launch, datetime.utcnow().isoformat()])
                seen.add(tn)
                written += 1
                total_saved += 1

        print(f"✅ Batch {batch_num} -> saved {written} new tools.")
        # brief pause to avoid hitting limits hard
        time.sleep(1.2)

    # update last offset - move forward PAGES_PER_RUN*RESULTS_PER_PAGE
    new_offset = start_offset + (PAGES_PER_RUN * RESULTS_PER_PAGE)
    save_last_offset(new_offset)
    save_seen(seen)
    print(f"🎯 Done. Total new tools saved this run: {total_saved}")
    print(f"Last offset updated to: {new_offset}. seen_tools: {len(seen)} entries.")