# mscraper.py  (updated)
# Final scraper helpers + robust GPT prompt + safe parsing
import os, time, json, csv, re
from urllib.parse import urlparse
import requests
import tldextract
import openai
from datetime import datetime

### -------- CONFIG --------
SERP_API_KEY = os.getenv("SERP_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

SERPAPI_URL = "https://serpapi.com/search.json"
RESULTS_PER_RUN = 100
PAGES_PER_RUN = 10
RESULTS_PER_PAGE = 10
BATCH_SIZE = 10              # you preferred 10 for accuracy
OUTPUT_FILE = "construction_tools.csv"
SEEN_FILE = "seen_tools.csv"
LAST_OFFSET_FILE = "last_offset.txt"
RATE_LIMIT_BACKOFF = 6

openai.api_key = OPENAI_API_KEY

# known reputable sites we want GPT to prefer as "source"
KNOWN_SOURCES = {
    "producthunt": "Product Hunt",
    "futuretools": "FutureTools",
    "theresanaiforthat": "There's An AI For That",
    "futurepedia": "Futurepedia",
    "g2.com": "G2",
    "product-hunt": "Product Hunt",
    "linkedin.com": "LinkedIn",
    "reddit.com": "Reddit",
    "github.com": "GitHub",
    "appsumo.com": "AppSumo",
    "angel.co": "AngelList",
    "crunchbase.com": "Crunchbase",
    "youtube.com": "YouTube",
    "medium.com": "Medium",
    "dev.to": "Dev.to",
    "hackernews": "Hacker News",
    "news": "News",
}

# ----------------- helpers -----------------
def ensure_output_exists():
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "tool_name",
                "description",
                "website",
                "source",
                "tags",
                "reviews",
                "launch_date",
                "scrape_date"
            ])

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
    # get first JSON object/array block
    m = re.search(r"(\[.*\]|\{.*\})", raw, re.DOTALL)
    return m.group(0) if m else None

def safe_get_str(obj, key):
    """Normalize a returned field to a simple trimmed string."""
    v = obj.get(key) if isinstance(obj, dict) else None
    if v is None:
        return ""
    if isinstance(v, list):
        parts = []
        for item in v:
            if item is None:
                continue
            parts.append(str(item).strip())
        return ", ".join([p for p in parts if p])
    if isinstance(v, dict):
        # serialize small dict to a compact JSON string
        try:
            return json.dumps(v, ensure_ascii=False)
        except:
            return str(v)
    return str(v).strip()

def classify_source(displayed_link_or_src, snippet, title, website_url=None):
    """
    Try to return a human-friendly 'source' name (Product Hunt, G2, Reddit, LinkedIn, Blog, Newsletter, etc.)
    Avoid returning the tool website as the source. If that would be the same, try to infer a better source from text.
    """
    s = (displayed_link_or_src or "").lower().strip()
    site_domain_of_tool = domain_from_url(website_url) if website_url else ""
    # 1) if s contains known source hint -> map
    for k, v in KNOWN_SOURCES.items():
        if k in s:
            return v
    # 2) check snippet/title for explicit mentions
    txt = ((snippet or "") + " " + (title or "")).lower()
    for k, v in KNOWN_SOURCES.items():
        if k in txt or v.lower() in txt:
            return v
    # 3) if s looks like a domain but equals the tool website domain -> prefer "Website" or try to find mention in text
    if s:
        ds = domain_from_url(s)
        if ds and site_domain_of_tool and ds == site_domain_of_tool:
            # same domain -> look for other site mentions in text
            for k, v in KNOWN_SOURCES.items():
                if k in txt or v.lower() in txt:
                    return v
            return "Website"
        # otherwise if s looks like a domain and not the same as tool domain -> return cleaned domain or mapped name
        if ds:
            # map domain to friendly name if known, else return domain
            for k, v in KNOWN_SOURCES.items():
                if k in ds:
                    return v
            return ds
    # final fallback
    if any(word in txt for word in ["newsletter", "subscribe", "issue #", "issue:"]):
        return "Newsletter"
    if any(word in txt for word in ["blog", "post", "article"]):
        return "Blog"
    if any(word in txt for word in ["tweet", "x.com", "twitter", "threads", "post on linkedin", "shared on linkedin"]):
        return "Social Media"
    return "Google Search"

# ---------------- GPT helpers ----------------
def safe_gpt_call(prompt, max_retries=4):
    """
    Try a few ways to call the OpenAI client to be compatible across versions.
    Returns the assistant text or None.
    """
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            # Preferred (older openai style)
            resp = openai.ChatCompletion.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            text = resp["choices"][0]["message"]["content"]
            return text
        except Exception as e1:
            # Try alternate new-client style if installed
            try:
                # newer openai python (client)
                from openai import OpenAI
                client = OpenAI(api_key=OPENAI_API_KEY)
                resp2 = client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0
                )
                # resp2.choices[0].message.content or similar depending on lib
                try:
                    return resp2.choices[0].message.content
                except:
                    # fallback to dict access
                    return resp2["choices"][0]["message"]["content"]
            except Exception:
                # last-resort: wait and retry
                wait = RATE_LIMIT_BACKOFF * attempt
                print(f"⚠️ GPT call failed (attempt {attempt}/{max_retries}): {e1} — retrying in {wait}s")
                time.sleep(wait)
    return None

def build_prompt(batch, batch_num):
    """
    Strict prompt instructing GPT to:
      - only use title/snippet/link/displayed_link fields
      - return a JSON array, each object MUST include tool_name to be included
      - produce 'source' as the site where the tool was mentioned (prefer Product Hunt, G2, FutureTools, etc.)
      - 'tags' should be a comma-separated string (not array)
      - 'reviews' numeric string, else "0"
    """
    payload = json.dumps(batch, ensure_ascii=False)
    instruction = f"""
You are an *extractor* that MUST return valid JSON only (a JSON ARRAY). Input is a JSON array with items that have: title, snippet, link, source/displayed_link.

Rules (STRICT):
- For each input item produce an object with keys:
  - tool_name (string)  -- REQUIRED. If you can't identify a tool name confidently from title/snippet, skip this item.
  - description (short string, 8-28 words) -- prefer the snippet trimmed to a concise sentence.
  - website (domain only, e.g. 'togal.ai') -- derive from the provided 'link' (NOT the same as 'source' if possible).
  - source (string) -- the place where this tool was *mentioned* (e.g. 'Product Hunt', 'G2', 'FutureTools', 'There's An AI For That', 'Reddit', 'LinkedIn', 'Medium', 'YouTube', 'GitHub', 'AppSumo', 'Blog', 'Newsletter', 'Google Search'). Prefer explicit mentions in title/snippet/displayed_link. If multiple are present, choose the most authoritative mention (Product Hunt > G2 > FutureTools > Reddit > LinkedIn > Medium > Blog > Website > Google Search).
  - tags (comma-separated keywords) -- derive keywords from title or snippet, max 6. Return as a single string (e.g. "project management, BIM, scheduling").
  - reviews (integer string) -- explicit numeric count if present, else "0".
  - launch_date (short month-year or year) -- if present in snippet/title, else empty string.

- IMPORTANT: Do NOT return arrays for 'tags' or other fields — return strings.
- ONLY use the provided fields. Do NOT invent features or numbers. If uncertain, put "0" (for reviews) or empty string for launch_date.
- Output only a JSON ARRAY of objects. No explanation, no markdown, no extra text.

Batch number: {batch_num}
INPUT:
{payload}
"""
    return instruction