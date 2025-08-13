# mscraper.py
# Multi-engine scraper + GPT extraction + Grok source-fix
# CSV columns: tool_name, description, website, source, tags, reviews, launch_date

import os, time, json, csv, re
from urllib.parse import urlparse, urlencode
import requests
import tldextract
from datetime import datetime

# ---------- CONFIG ----------
SERP_API_KEY   = os.getenv("SERP_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GROK_API_KEY   = os.getenv("GROK_API_KEY", "")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GOOGLE_CSE_ID  = os.getenv("GOOGLE_CSE_ID", "")
BING_API_KEY   = os.getenv("BING_API_KEY", "")

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # OpenAI v1 model

# Scraping
RESULTS_PER_RUN   = 100
PAGES_PER_RUN     = 10
RESULTS_PER_PAGE  = 10
BATCH_SIZE        = 10

OUTPUT_FILE       = "construction_tools.csv"
SEEN_FILE         = "seen_tools.csv"
LAST_OFFSET_FILE  = "last_offset.txt"
RATE_LIMIT_BACKOFF = 8

# default query (app.py can override mscraper.QUERY)
QUERY = "construction AI tools"

# ---------- Reputable sources (labels -> domains) ----------
REPUTABLE_SOURCES = {
    "Product Hunt":      ["producthunt.com"],
    "G2":                ["g2.com"],
    "Capterra":          ["capterra.com"],
    "GetApp":            ["getapp.com"],
    "Crunchbase":        ["crunchbase.com"],
    "AngelList":         ["angel.co", "wellfound.com"],
    "GitHub":            ["github.com"],
    "GitLab":            ["gitlab.com"],
    "StackShare":        ["stackshare.io"],
    "BetaList":          ["betalist.com"],
    "Indie Hackers":     ["indiehackers.com"],
    "Reddit":            ["reddit.com"],
    "LinkedIn":          ["linkedin.com"],
    "Hacker News":       ["news.ycombinator.com", "ycombinator.com"],
    "Medium":            ["medium.com"],
    "Dev.to":            ["dev.to"],
    "SitePoint":         ["sitepoint.com"],
    "InfoQ":             ["infoq.com"],
    "The Verge":         ["theverge.com"],
    "TechCrunch":        ["techcrunch.com"],
    "ZDNet":             ["zdnet.com"],
    "Wired":             ["wired.com"],
    "Ars Technica":      ["arstechnica.com"],
    "YouTube":           ["youtube.com", "youtu.be"],
    "X/Twitter":         ["twitter.com", "x.com"],
    "Docs":              ["readthedocs.io", "docs.google.com"],
    "Notion":            ["notion.site", "notion.so"],
    "Substack":          ["substack.com"],
}

# flatten domain list
REPUTABLE_DOMAIN_SET = set(d for ds in REPUTABLE_SOURCES.values() for d in ds)

# ---------- Utilities ----------
def ensure_output_exists():
    """Create CSV with 7 headers if missing."""
    if not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "tool_name",
                "description",
                "website",
                "source",
                "tags",
                "reviews",
                "launch_date"
            ])

def load_seen():
    s = set()
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            for line in f:
                v = line.strip()
                if v:
                    s.add(v.lower())
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
        netloc = urlparse(url).netloc.lower().replace("www.", "")
        return netloc
    except:
        return ""

def safe_get_str(obj, key):
    """Safely get string for a dict field; join lists/dicts -> string."""
    if not isinstance(obj, dict):
        return ""
    val = obj.get(key, "")
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, list):
        return " ".join([str(x) for x in val])
    if isinstance(val, dict):
        return json.dumps(val, ensure_ascii=False)
    return str(val).strip()

def looks_reputable(url_or_domain):
    d = domain_from_url(url_or_domain)
    if not d:
        return False
    return any(d == rd or d.endswith("." + rd) for rd in REPUTABLE_DOMAIN_SET)

def make_google_query_url(q):
    return "https://www.google.com/search?q=" + urlencode({"q": q})[2:]

# ---------- Engines ----------
def fetch_serpapi(start_offset):
    if not SERP_API_KEY:
        return []
    SERPAPI_URL = "https://serpapi.com/search.json"
    results = []
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
            r = requests.get(SERPAPI_URL, params=params, timeout=25)
            r.raise_for_status()
            data = r.json()
            hits = data.get("organic_results") or []
            for h in hits:
                results.append({
                    "title": (h.get("title") or "").strip(),
                    "snippet": (h.get("snippet") or "").strip(),
                    "link": (h.get("link") or h.get("url") or "").strip(),
                    "displayed_link": (h.get("displayed_link") or "").strip(),
                    "engine": "serpapi"
                })
            time.sleep(0.6)
        except Exception as e:
            print(f"❌ SerpAPI failed @start={offset}: {e}")
            time.sleep(1.5)
            continue
    return results

def fetch_google_cse(start_offset):
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return []
    base = "https://www.googleapis.com/customsearch/v1"
    results = []
    for page in range(PAGES_PER_RUN):
        start = start_offset + page * RESULTS_PER_PAGE + 1  # CSE is 1-based start
        params = {
            "key": GOOGLE_API_KEY,
            "cx": GOOGLE_CSE_ID,
            "q": QUERY,
            "start": start,
            "num": RESULTS_PER_PAGE
        }
        try:
            r = requests.get(base, params=params, timeout=25)
            r.raise_for_status()
            data = r.json()
            items = data.get("items", [])
            for it in items:
                results.append({
                    "title": (it.get("title") or "").strip(),
                    "snippet": (it.get("snippet") or "").strip(),
                    "link": (it.get("link") or "").strip(),
                    "displayed_link": (it.get("displayLink") or "").strip(),
                    "engine": "google_cse"
                })
            time.sleep(0.6)
        except Exception as e:
            print(f"❌ Google CSE failed @start={start}: {e}")
            time.sleep(1.5)
            continue
    return results

def fetch_bing(start_offset):
    if not BING_API_KEY:
        return []
    base = "https://api.bing.microsoft.com/v7.0/search"
    headers = {"Ocp-Apim-Subscription-Key": BING_API_KEY}
    results = []
    for page in range(PAGES_PER_RUN):
        offset = start_offset + page * RESULTS_PER_PAGE
        params = {
            "q": QUERY,
            "count": RESULTS_PER_PAGE,
            "offset": offset,
            "textDecorations": False,
            "textFormat": "HTML",
            "mkt": "en-US"
        }
        try:
            r = requests.get(base, headers=headers, params=params, timeout=25)
            r.raise_for_status()
            data = r.json()
            webPages = data.get("webPages", {}).get("value", [])
            for w in webPages:
                results.append({
                    "title": (w.get("name") or "").strip(),
                    "snippet": (w.get("snippet") or "").strip(),
                    "link": (w.get("url") or "").strip(),
                    "displayed_link": domain_from_url(w.get("url") or ""),
                    "engine": "bing"
                })
            time.sleep(0.6)
        except Exception as e:
            print(f"❌ Bing failed @offset={offset}: {e}")
            time.sleep(1.5)
            continue
    return results

def aggregate_results(start_offset):
    """Collect from all available engines, dedupe by link+title."""
    bag = []
    bag.extend(fetch_serpapi(start_offset))
    bag.extend(fetch_google_cse(start_offset))
    bag.extend(fetch_bing(start_offset))

    # fallback: if none available, return empty
    if not bag:
        print("⚠️ No search engines available (missing API keys).")
        return []

    dedup = []
    seen = set()
    for r in bag:
        key = (r.get("link") or "") + "||" + (r.get("title") or "")
        if key in seen:
            continue
        seen.add(key)
        dedup.append(r)
    return dedup

# ---------- OpenAI (v1) + Grok helpers ----------
_openai_client = None
def get_openai_client():
    global _openai_client
    if _openai_client or not OPENAI_API_KEY:
        return _openai_client
    try:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=OPENAI_API_KEY)
        return _openai_client
    except Exception as e:
        print("⚠️ OpenAI v1 client not available:", e)
        return None

def safe_gpt_call(prompt, max_retries=5, temperature=0):
    """OpenAI v1 Chat Completions."""
    client = get_openai_client()
    if not client:
        return None
    attempt = 0
    while attempt < max_retries:
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role":"user","content": prompt}],
                temperature=temperature
            )
            return resp.choices[0].message.content
        except Exception as e:
            attempt += 1
            wait = RATE_LIMIT_BACKOFF * attempt
            print(f"⚠️ GPT call failed ({attempt}/{max_retries}): {e} → retry in {wait}s")
            time.sleep(wait)
    return None

def grok_complete(prompt, max_retries=4, temperature=0):
    """Call xAI Grok chat completions via requests."""
    if not GROK_API_KEY:
        return None
    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROK_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "grok-2-latest",
        "messages": [{"role":"user","content": prompt}],
        "temperature": temperature
    }
    attempt = 0
    while attempt < max_retries:
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=40)
            r.raise_for_status()
            data = r.json()
            return data.get("choices", [{}])[0].get("message", {}).get("content", "")
        except Exception as e:
            attempt += 1
            wait = RATE_LIMIT_BACKOFF * attempt
            print(f"⚠️ Grok call failed ({attempt}/{max_retries}): {e} → retry in {wait}s")
            time.sleep(wait)
    return None

# ---------- Prompts ----------
def build_prompt(batch, batch_num):
    # Batch: list of dict(title, snippet, link, displayed_link, engine)
    payload = json.dumps(batch, ensure_ascii=False)
    # Provide reputable domains to prefer for "source_url"
    preferred = ", ".join(sorted(REPUTABLE_DOMAIN_SET))
    return f"""
You are a strict extractor from search results. INPUT is a JSON array of up to {BATCH_SIZE} search items:
Each item has: title, snippet, link, displayed_link, engine.

For each item, EXTRACT ONLY if you can confidently identify a tool/product name in title/snippet.
Do NOT invent names. Keep it concise.

Return STRICT JSON ARRAY (no commentary). Each object MUST have:
- tool_name (string)
- description (8–30 words)
- website (domain only, from the most likely official site or the link domain if unknown)
- source (URL) — a reputable third-party page about the tool (e.g., producthunt, g2, crunchbase, reddit, linkedin, medium, youtube, news). 
  It MUST NOT be the same domain as 'website'.
  Prefer domains from: {preferred}. If none obvious, output a Google search URL like:
  "https://www.google.com/search?q=<tool name>%20Product%20Hunt%20G2"
- tags (comma-separated, <=6, from title/snippet; default "AI, construction" if unsure)
- reviews (integer string if any number of reviews/ratings/users is stated; else "0")
- launch_date (month-year or year if hinted; else "")

Rules:
- JSON ARRAY only. If tool_name not clear, SKIP item.
- Do not add extra fields.
- For website: use domain only (e.g., "togal.ai"). Derive from official if obvious; otherwise domain_from_url(link).
- For source: use a different domain than website and prefer reputable sources or a Google search URL if none found.

Batch {batch_num}.
INPUT:
{payload}
"""

def build_grok_source_fix_prompt(item):
    """
    item is a dict with tool_name, website, source, tags, reviews, launch_date, description.
    Goal: if source is empty or repeats website domain, propose ONE reputable URL (NOT same domain as website).
    """
    preferred = ", ".join(sorted(REPUTABLE_DOMAIN_SET))
    name = safe_get_str(item, "tool_name")
    website = safe_get_str(item, "website")
    desc = safe_get_str(item, "description")
    tags = safe_get_str(item, "tags")

    q = f"{name} reviews producthunt g2 crunchbase reddit linkedin youtube"
    g_url = make_google_query_url(q)

    return f"""
You must return a single URL (nothing else) to a reputable third-party page about the tool below.

Tool name: {name}
Website domain: {website}
Short description: {desc}
Tags: {tags}

Requirements:
- The URL MUST NOT be on the same domain as the website.
- Prefer domains from this set: {preferred}
- Good choices: Product Hunt, G2, Crunchbase, AngelList/Wellfound, GitHub, Reddit, LinkedIn, Medium, HN, YouTube, Capterra, GetApp, etc.
- If you cannot find a single clear reputable page from your knowledge, return this Google search:
{g_url}

Return only the URL, nothing else.
"""

# ---------- Main scrape helpers ----------
def run_multi_search(start_offset):
    """Aggregate results from available engines and return unique list."""
    return aggregate_results(start_offset)

def parse_gpt_json(raw_text):
    if not raw_text:
        return []
    m = re.search(r"(\[.*\]|\{.*\})", raw_text, re.DOTALL)
    if not m:
        return []
    try:
        data = json.loads(m.group(0))
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            return data
    except:
        return []
    return []

def normalize_item(obj):
    """Ensure fields are strings, reviews numeric, tags fallback."""
    tn = safe_get_str(obj, "tool_name")
    desc = safe_get_str(obj, "description")
    web = safe_get_str(obj, "website")
    src = safe_get_str(obj, "source")
    tags = safe_get_str(obj, "tags")
    reviews = safe_get_str(obj, "reviews")
    launch = safe_get_str(obj, "launch_date")

    # digits only for reviews
    if not re.match(r"^\d+$", reviews or ""):
        # try extract from desc
        reviews_guess = extract_review_count(desc)
        reviews = reviews_guess if reviews_guess else "0"

    # fallback tags
    if not tags:
        tags = "AI, construction"

    # Ensure website is domain only
    web_domain = domain_from_url(web) if "." in web else web
    if not web_domain:
        web_domain = domain_from_url(src) or domain_from_url(web) or ""

    return {
        "tool_name": tn,
        "description": desc,
        "website": web_domain,
        "source": src,
        "tags": tags,
        "reviews": reviews,
        "launch_date": launch
    }

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

def sources_need_fix(item):
    """True if source missing or shares domain with website."""
    src = safe_get_str(item, "source")
    web = safe_get_str(item, "website")
    if not src:
        return True
    d_src = domain_from_url(src)
    d_web = domain_from_url(web)
    return d_src and d_web and (d_src == d_web)

def improve_sources_with_grok(items):
    """For items where source is missing/same-domain, ask Grok to produce a reputable URL."""
    if not GROK_API_KEY:
        return items
    improved = []
    for it in items:
        if sources_need_fix(it):
            prompt = build_grok_source_fix_prompt(it)
            out = grok_complete(prompt, temperature=0)
            if out:
                out = out.strip().split()[0]
                # sanity: if still same domain as website, fallback to Google search
                if domain_from_url(out) == domain_from_url(it.get("website", "")):
                    out = make_google_query_url(f"{it.get('tool_name','')} reviews producthunt g2")
                it["source"] = out
        improved.append(it)
    return improved

# ---------- Public run function used by app.py ----------
def run_scrape(query, mode="Resume"):
    """
    Returns (total_saved, last_offset, parsed_items) and writes CSV lines.
    CSV headers fixed: 7 columns.
    """
    global QUERY
    QUERY = query or QUERY

    ensure_output_exists()
    seen_names = load_seen()
    last_offset = load_last_offset()
    start_offset = last_offset if mode.lower().startswith("resume") else 0

    print(f"🔍 Fetching up to {RESULTS_PER_RUN} results starting @offset {start_offset}...")
    raw_results = run_multi_search(start_offset)
    print(f"⚙️ Collected {len(raw_results)} raw results from engines.")

    # first dedupe already done in aggregate_results; now optional naive seen filter
    candidates = raw_results

    # Process in GPT batches
    total_saved = 0
    batch_num = 0
    all_parsed = []

    for i in range(0, len(candidates), BATCH_SIZE):
        batch = candidates[i:i+BATCH_SIZE]
        batch_num += 1
        print(f"⚙️ Batch {batch_num} ({i+1}-{i+len(batch)}) → GPT...")

        prompt = build_prompt(batch, batch_num)
        raw = safe_gpt_call(prompt, max_retries=5, temperature=0)

        parsed = parse_gpt_json(raw)
        if not parsed:
            print("⚠️ Empty/invalid GPT JSON for this batch.")
            continue

        # Normalize + quick fixes
        normalized = [normalize_item(p) for p in parsed if isinstance(p, dict)]

        # Grok improve sources if needed
        normalized = improve_sources_with_grok(normalized)

        # Write to CSV with dedupe by tool_name + website domain
        written = 0
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for obj in normalized:
                tn   = safe_get_str(obj, "tool_name")
                desc = safe_get_str(obj, "description")
                web  = safe_get_str(obj, "website")
                src  = safe_get_str(obj, "source")
                tags = safe_get_str(obj, "tags")
                rev  = safe_get_str(obj, "reviews")
                ld   = safe_get_str(obj, "launch_date")

                if not (tn and desc and web and src):
                    # All required; do not save empty
                    continue

                # Avoid same-domain src
                if domain_from_url(src) == domain_from_url(web):
                    # last safety fallback
                    src = make_google_query_url(f"{tn} reviews producthunt g2")

                # final dedupe
                if tn.lower() in seen_names:
                    continue

                w.writerow([tn, desc, web, src, tags, rev, ld])
                seen_names.add(tn.lower())
                written += 1
                total_saved += 1
                all_parsed.append(obj)

        print(f"✅ Batch {batch_num}: saved {written} new tools.")
        time.sleep(1.0)

    # move offset forward by configured pages*results
    new_offset = start_offset + (PAGES_PER_RUN * RESULTS_PER_PAGE)
    save_last_offset(new_offset)
    save_seen(seen_names)

    print(f"🎯 Done. Total new tools saved: {total_saved}. New offset: {new_offset}")
    return total_saved, new_offset, all_parsed