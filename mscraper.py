# mscraper.py
# Updated scraper with memory-based source guessing rules and looser GPT filtering
# Always tries to return at least some results

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

load_dotenv()

# -------- CONFIG --------
SERP_API_KEY = os.getenv("SERP_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
SERPAPI_URL = "https://serpapi.com/search.json"

RESULTS_PER_RUN = 100
PAGES_PER_RUN = 10
RESULTS_PER_PAGE = 10
BATCH_SIZE = 10
OUTPUT_FILE = "construction_tools.csv"
SEEN_FILE = "seen_tools.csv"
LAST_OFFSET_FILE = "last_offset.txt"
RATE_LIMIT_BACKOFF = 8
MAX_NEW_PER_RUN = 200

# ---------- KNOWN / TRUSTED SOURCES ----------
KNOWN_SOURCES = {
    "producthunt": "Product Hunt",
    "g2.com": "G2",
    "an.ai.for.that": "TheresAnAIForThat",
    "futurepedia.io": "Futurepedia",
    "futuretools.io": "FutureTools",
    "crunchbase.com": "Crunchbase",
    "angel.co": "AngelList",
    "appsumo.com": "AppSumo",
    "linkedin.com": "LinkedIn",
    "reddit.com": "Reddit",
    "medium.com": "Medium",
    "youtube.com": "YouTube",
    "builtworlds.com": "BuiltWorlds",
    "github.com": "GitHub",
    "dev.to": "Dev.to",
    "autodesk.com": "Autodesk",
    "constructiondive.com": "Construction Dive",
    "enr.com": "ENR",
    "archdaily.com": "ArchDaily",
    "bimforum.org": "BIMForum",
    "forconstructionpros.com": "ForConstructionPros",
    "bdcnetwork.com": "Building Design + Construction",
    "constructionexec.com": "Construction Executive"
}

# ---------- Helpers ----------
def ensure_output_exists():
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["tool_name", "description", "website", "source", "tags", "reviews", "launch_date"])

def load_seen():
    if os.path.exists(SEEN_FILE):
        return set(open(SEEN_FILE, "r", encoding="utf-8").read().splitlines())
    return set()

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
    open(LAST_OFFSET_FILE, "w", encoding="utf-8").write(str(int(offset)))

def domain_from_url(url):
    try:
        ex = tldextract.extract(url)
        return f"{ex.domain}.{ex.suffix}".lower() if ex.domain and ex.suffix else ""
    except:
        return ""

# ---------- Source Guessing (Memory Rules) ----------
def classify_source(displayed_link, snippet, title, website_domain=None):
    # Rule 1: Primary match from known list
    s_link = (displayed_link or "").lower()
    for k, v in KNOWN_SOURCES.items():
        if k in s_link and (not website_domain or k not in website_domain):
            return v

    # Rule 2: Secondary match from snippet/title clues
    text = f"{snippet or ''} {title or ''}".lower()
    for k, v in KNOWN_SOURCES.items():
        if k.replace(".com", "") in text and (not website_domain or k not in website_domain):
            return v

    # Rule 3: Domain separation
    link_domain = domain_from_url(s_link)
    if website_domain and link_domain == website_domain:
        pass
    elif link_domain:
        return link_domain

    # Rule 4: Last fallback
    return "General Web"

# ---------- OPENAI Setup ----------
try:
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    new_openai = True
except:
    import openai
    openai.api_key = OPENAI_API_KEY
    client = openai
    new_openai = False

def safe_gpt_call(prompt):
    for attempt in range(5):
        try:
            if new_openai:
                resp = client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    max_tokens=1200
                )
                return resp.choices[0].message.content.strip()
            else:
                resp = client.ChatCompletion.create(
                    model=OPENAI_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    max_tokens=1200
                )
                return resp["choices"][0]["message"]["content"].strip()
        except Exception as e:
            print(f"⚠ GPT call failed ({e}), retrying...")
            time.sleep(RATE_LIMIT_BACKOFF * (attempt+1))
    return None

# ---------- Prompt ----------
def build_prompt(batch):
    payload = json.dumps(batch, ensure_ascii=False)
    return f"""
You are extracting AI and construction tool data.
For each JSON item, return: tool_name, description, website, source, tags, reviews, launch_date.
Rules:
- Always extract tool_name if reasonably guessable from title.
- If website missing, use domain from link.
- Source must follow rules: prefer reputable sources, avoid using tool's own domain, fallback to 'General Web'.
- tags: up to 6 relevant keywords from title/snippet, else 'AI, construction'.
- reviews: number if found, else "0".
- launch_date: short month-year or year, else "".
Return STRICT JSON array.
INPUT:
{payload}
"""

# ---------- SERP Fetch ----------
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
            r.raise_for_status()
            data = r.json()
            for h in data.get("organic_results", []):
                results.append({
                    "title": h.get("title", ""),
                    "snippet": h.get("snippet", ""),
                    "link": h.get("link", ""),
                    "displayed_link": h.get("displayed_link", "")
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"❌ SerpAPI fetch failed: {e}")
    return results

# ---------- Main ----------
def run_once(query, resume=True):
    ensure_output_exists()
    seen = load_seen()
    last_offset = load_last_offset()
    start_offset = last_offset if resume else 0

    print(f"🔍 Fetching results for '{query}' from offset {start_offset}")
    raw_results = run_serpapi_pages(start_offset, query)
    print(f"⚙ Collected {len(raw_results)} raw results")

    candidates = []
    for r in raw_results:
        website_domain = domain_from_url(r["link"])
        src = classify_source(r["displayed_link"], r["snippet"], r["title"], website_domain)
        candidates.append({
            "title": r["title"],
            "snippet": r["snippet"],
            "link": r["link"],
            "source": src
        })

    print(f"⚙ Processing {len(candidates)} candidates via GPT...")
    new_tools = 0

    for i in range(0, len(candidates), BATCH_SIZE):
        batch = candidates[i:i+BATCH_SIZE]
        gpt_out = safe_gpt_call(build_prompt(batch))
        if not gpt_out:
            print("⚠ GPT failed, skipping batch.")
            continue
        try:
            data = json.loads(re.sub(r"```(?:json)?|```", "", gpt_out))
        except Exception as e:
            print(f"⚠ JSON parse error: {e}")
            continue

        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for obj in data:
                tn = obj.get("tool_name", "").strip()
                if not tn or tn.lower() in (s.lower() for s in seen):
                    continue
                writer.writerow([
                    tn,
                    obj.get("description", ""),
                    obj.get("website", ""),
                    obj.get("source", ""),
                    obj.get("tags", "AI, construction"),
                    obj.get("reviews", "0"),
                    obj.get("launch_date", "")
                ])
                seen.add(tn)
                new_tools += 1

    save_seen(seen)
    save_last_offset(start_offset + RESULTS_PER_RUN)
    print(f"🎯 Saved {new_tools} new tools this run.")

if __name__ == "__main__":
    q = input("Search query (default: 'construction AI tools'): ").strip() or "construction AI tools"
    run_once(q, resume=True)