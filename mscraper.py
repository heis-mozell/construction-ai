# mscraper.py
# Merged + fixed version of your scraper
# - Writes 7 columns: tool_name, description, website, source, tags, reviews, launch_date
# - BATCH_SIZE = 10
# - Resume support, dedupe by tool_name, robust GPT calls (supports both old/new openai libs)
# - Source classification prefers reputable-known sources and avoids using the tool's own domain as source

import os
import time
import json
import csv
import re
import requests
import tldextract
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()  # optional - only if you use a .env file locally

# -------- CONFIG --------
SERP_API_KEY = os.getenv("SERP_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # keep your chosen model
SERPAPI_URL = "https://serpapi.com/search.json"

RESULTS_PER_RUN = 100     # default, 10 pages * 10
PAGES_PER_RUN = 10
RESULTS_PER_PAGE = 10
BATCH_SIZE = 10           # as you asked
OUTPUT_FILE = "construction_tools.csv"
SEEN_FILE = "seen_tools.csv"
LAST_OFFSET_FILE = "last_offset.txt"
RATE_LIMIT_BACKOFF = 8
MAX_NEW_PER_RUN = 200     # safety cap (won't save more than this in a single run)

# ---------- KNOWN / TRUSTED SOURCES (you can extend) ----------
KNOWN_SOURCES = {
    "producthunt": "Product Hunt",
    "g2.com": "G2",
    "futurepedia.io": "Futurepedia",
    "futuretools.io": "FutureTools",
    "theresanaiforthat": "TheresAnAIForThat",
    "reddit.com": "Reddit",
    "linkedin.com": "LinkedIn",
    "twitter.com": "Twitter/X",
    "x.com": "Twitter/X",
    "medium.com": "Medium",
    "youtube.com": "YouTube",
    "github.com": "GitHub",
    "dev.to": "Dev.to",
    "autodesk.com": "Autodesk",
    "constructiondive.com": "Construction Dive",
    "enr.com": "ENR",
    "archdaily.com": "ArchDaily",
    "bimforum.org": "BIMForum",
    "forconstructionpros.com": "ForConstructionPros",
    "buildingdesignandconstruction": "Building Design + Construction",
    "constructionexec.com": "Construction Executive",
}

# ---------- Helpers ----------
def ensure_output_exists():
    # header: exactly 7 columns required by your app:
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["tool_name", "description", "website", "source", "tags", "reviews", "launch_date"])

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
    m = re.search(r"(\d{1,3}(?:[,\s]\d{3})*)\s+(?:reviews|ratings|votes|ratings?)", text, re.IGNORECASE)
    if m:
        return re.sub(r"[,\s]", "", m.group(1))
    m2 = re.search(r"(\d{1,3}(?:[,\s]\d{3})*)\s+(?:users|customers|clients)", text, re.IGNORECASE)
    if m2:
        return re.sub(r"[,\s]", "", m2.group(1))
    return "0"

def classify_source(displayed_link, snippet, title, fallback_domain=None):
    """
    Determine the best 'source' for a SERP item.
    - Prefer known sources mapped in KNOWN_SOURCES.
    - Do not return the same domain as the tool's website (fallback_domain).
    - If nothing found, return 'Google Search'.
    """
    s = (displayed_link or "").lower()
    # check displayed link domain for known sources
    for k, v in KNOWN_SOURCES.items():
        if k in s:
            if fallback_domain and k in fallback_domain:
                continue
            return v

    # check snippet / title
    txt = f"{snippet or ''} {title or ''}".lower()
    for k, v in KNOWN_SOURCES.items():
        if k.replace(".com", "") in txt or k.replace(".io", "") in txt:
            # don't return the same domain as website
            if fallback_domain and k in fallback_domain:
                continue
            return v

    # fallback: if displayed_link looks like another domain and not equal to website
    if s:
        ds = s.replace("www.", "")
        if fallback_domain and ds == fallback_domain:
            return "Google Search"
        # pick a cleaned domain
        return ds

    return "Google Search"

# ---------- OPENAI compatibility (works for openai>=1.0 or older) ----------
# We'll try to use the new client if available; else fallback to older interface.
_openai_client = None
_new_openai = False
try:
    # new style
    from openai import OpenAI
    _openai_client = OpenAI(api_key=OPENAI_API_KEY)
    _new_openai = True
except Exception:
    try:
        import openai as _old_openai
        _old_openai.api_key = OPENAI_API_KEY
        _openai_client = _old_openai
        _new_openai = False
    except Exception:
        _openai_client = None
        _new_openai = False

def clean_json_from_gpt(raw):
    if not raw:
        return None
    # remove code fences and try to find first JSON block
    raw = re.sub(r"```(?:json)?", "", raw)  # strip triple backticks
    m = re.search(r"(.*|\{.*\})", raw, re.DOTALL)
    if not m:
        return None
    return m.group(0)

def safe_gpt_call(prompt, max_retries=5, backoff_base=RATE_LIMIT_BACKOFF, timeout=30):
    """
    Universal GPT call that supports both old and new openai libraries.
    Returns response string or None.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            if _openai_client is None:
                raise RuntimeError("OpenAI client not configured (OPENAI_API_KEY missing or openai import failed).")
            if _new_openai:
                # new library: client.chat.completions.create(...)
                resp = _openai_client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    max_tokens=1200
                )
                # response text:
                text = ""
                if resp and getattr(resp, "choices", None):
                    # choices[0].message.content
                    c = resp.choices[0]
                    text = getattr(getattr(c, "message", None), "content", "") or ""
                else:
                    text = str(resp)
                return text
            else:
                # old openai package
                resp = _openai_client.ChatCompletion.create(
                    model=OPENAI_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    max_tokens=1200
                )
                # resp is dict-like
                return resp["choices"][0]["message"]["content"]
        except Exception as e:
            attempt += 1
            wait = backoff_base * attempt
            print(f"⚠️ GPT call failed (attempt {attempt}/{max_retries}): {e}. backing off {wait}s...")
            time.sleep(wait)
            continue
    return None

# ---------- Prompt builder ----------
def build_prompt(batch, batch_num, known_sources_list=None):
    """
    batch: list of dicts each with title, snippet, link, source (candidate)
    """
    known_list = known_sources_list if known_sources_list else list(KNOWN_SOURCES.values())
    payload = json.dumps(batch, ensure_ascii=False)
    prompt = f"""
You are a strict extractor. INPUT is a JSON array of up to {BATCH_SIZE} search results (each item: title, snippet, link, source).
For each item, extract information ONLY from the fields provided (title, snippet, link, source).
Rules:
- Extract only items where you can confidently determine a canonical tool/product NAME (tool_name).
- If the 'title' clearly contains the tool name, use it.
- Do NOT invent tool names.
- For website: return only the domain (example: 'fleetcommand.io').
- For source: try to identify where this result came from (prefer reputable sources: {known_list}).
  If the source is the same domain as the tool's website, try to find a different reputable source (Product Hunt, G2, Medium, Reddit, LinkedIn, YouTube) from the snippet/title.
  If you cannot find a reputable source, return 'Google Search'.
- tags: comma-separated keywords (0-6) derived only from title/snippet; if none found, return "AI, construction"
- reviews: integer as string if explicit, else "0"
- launch_date: short month-year (e.g. "Mar 2024") or year "2022", else empty string ""

Output:
Return STRICT JSON only: a JSON array of objects. Each object must have:
tool_name, description, website, source, tags, reviews, launch_date

If tool_name cannot be determined, SKIP that item.

Batch number: {batch_num}
INPUT:
{payload}
"""
    return prompt

# ---------- SERP fetcher ----------
def run_serpapi_pages(start_offset, query):
    all_results = []
    for page in range(PAGES_PER_RUN):
        offset = start_offset + page * RESULTS_PER_PAGE
        params = {
            "engine": "google",
            "q": query,
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
            time.sleep(0.5)
        except Exception as e:
            print(f"❌ SerpAPI page fetch failed at start={offset}: {e}")
            time.sleep(2)
            continue
    return all_results

# ---------- Main function to run a full scraping pass ----------
def run_once(query, resume=True, verbose=True):
    """
    query: search query string
    resume: if True resume from last_offset, else start at 0
    returns: number of new tools saved
    """
    ensure_output_exists()
    seen = load_seen()
    last_offset = load_last_offset()
    start_offset = last_offset if resume else 0

    if verbose:
        print(f"🔍 Fetching up to {RESULTS_PER_RUN} results from SerpAPI for query: {query} starting at {start_offset}")

    raw_results = run_serpapi_pages(start_offset, query)
    if verbose:
        print(f"⚙️ Collected {len(raw_results)} raw SERP items")

    # quick dedupe by link+title
    unique = []
    seen_keys = set()
    for r in raw_results:
        key = (r.get("link") or "") + "||" + (r.get("title") or "")
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique.append(r)
    if verbose:
        print(f"⚙️ {len(unique)} unique SERP candidates after dedupe")

    # prepare candidates: determine fallback domain for the website so classify_source can avoid it
    candidates = []
    for it in unique:
        link_domain = domain_from_url(it.get("link") or "")
        src_guess = classify_source(it.get("displayed_link", ""), it.get("snippet", ""), it.get("title", ""), fallback_domain=link_domain)
        candidates.append({
            "title": it.get("title", ""),
            "snippet": it.get("snippet", ""),
            "link": it.get("link", ""),
            "source": src_guess
        })

    # naive filter: skip items where title-derived naive tool name already seen
    filtered = []
    seen_lower = set(s.lower() for s in seen)
    for c in candidates:
        naive_name = (c["title"] or "").split("—")[0].split("|")[0].strip()
        if naive_name and naive_name.lower() in seen_lower:
            continue
        filtered.append(c)
    if verbose:
        print(f"⚙️ {len(filtered)} candidates passed naive seen check (final dedupe after GPT)")

    # Process in batches and call GPT
    total_saved = 0
    new_tools_this_run = 0
    batch_num = 0

    for i in range(0, len(filtered), BATCH_SIZE):
        if new_tools_this_run >= MAX_NEW_PER_RUN:
            if verbose:
                print("Reached MAX_NEW_PER_RUN cap; stopping further saves.")
            break

        batch = filtered[i:i + BATCH_SIZE]
        batch_num += 1
        if verbose:
            print(f"⚙️ Batch {batch_num}: processing {len(batch)} items with GPT...")

        prompt = build_prompt(batch, batch_num, known_sources_list=list(KNOWN_SOURCES.values()))
        raw_gpt = safe_gpt_call(prompt)
        if not raw_gpt:
            if verbose:
                print("⚠️ No GPT output for this batch — skipping.")
            time.sleep(1)
            continue

        json_text = clean_json_from_gpt(raw_gpt)
        if not json_text:
            if verbose:
                print("⚠️ GPT returned no JSON block — skipping batch.")
            time.sleep(1)
            continue

        try:
            parsed = json.loads(json_text)
            if not isinstance(parsed, list):
                if verbose:
                    print("⚠️ GPT JSON is not an array — skipping")
                continue
        except Exception as e:
            if verbose:
                print("⚠️ Failed to parse GPT JSON:", e)
            continue

        # write validated rows
        written = 0
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for obj in parsed:
                if not isinstance(obj, dict):
                    continue
                # normalize fields robustly (if a list is returned, join with commas)
                def _get_str(o, k):
                    v = o.get(k, "")
                    if v is None:
                        return ""
                    if isinstance(v, list):
                        return ", ".join([str(x) for x in v])
                    return str(v).strip()

                tn = _get_str(obj, "tool_name")
                desc = _get_str(obj, "description")
                web = _get_str(obj, "website")
                src = _get_str(obj, "source")
                tags = _get_str(obj, "tags")
                reviews = _get_str(obj, "reviews")
                launch = _get_str(obj, "launch_date")

                # mandatory
                if not tn:
                    continue  # must have name
                if not web:
                    # try derive website from the batch item link (if possible)
                    # find matching batch item by title snippet
                    # safer to skip than invent
                    continue
                if not src:
                    src = "Google Search"

                # normalize reviews
                if not re.match(r"^\d+$", reviews or ""):
                    reviews = extract_review_count(desc + " " + (batch[0].get("snippet", "") if batch else "")) or "0"

                if not tags:
                    tags = "AI, construction"

                # ensure source is not same as website domain
                web_dom = domain_from_url(web)
                src_low = src.lower()
                if web_dom and web_dom in src_low:
                    # attempt to find alternate source in snippet/title using known sources
                    alt = classify_source(obj.get("source", ""), desc, tn, fallback_domain=web_dom)
                    if alt and alt.lower() != web_dom.lower():
                        src = alt
                    else:
                        src = "Google Search"

                # dedupe by tool name
                if tn.lower() in seen_lower:
                    continue

                # finally write row in the exact order you requested (7 columns)
                writer.writerow([tn, desc, web, src, tags, reviews, launch])
                seen.add(tn)
                seen_lower.add(tn.lower())
                written += 1
                total_saved += 1
                new_tools_this_run += 1

                if new_tools_this_run >= MAX_NEW_PER_RUN:
                    break

        if verbose:
            print(f"✅ Batch {batch_num} saved {written} new tools.")
        time.sleep(1.2)

    # update offset and seen
    new_offset = start_offset + (PAGES_PER_RUN * RESULTS_PER_PAGE)
    save_last_offset(new_offset)
    save_seen(seen)

    if verbose:
        print(f"🎯 Done. Total new tools saved this run: {total_saved}. Last offset set to {new_offset}")
    return total_saved

# helper to allow calling from other modules
if __name__ == "__main__":
    # quick CLI mode
    q = input("Search query (default: 'construction AI tools'): ").strip() or "construction AI tools"
    resume_input = input("Resume (Y/n)? ").strip().lower() or "y"
    resume = resume_input.startswith("y")
    run_once(q, resume=resume, verbose=True)