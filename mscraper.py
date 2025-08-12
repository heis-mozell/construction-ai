# mscraper.py — Construction AI Scraper with strong source detection + GPT fallback
import os, time, json, csv, re
from urllib.parse import urlparse
import requests
import tldextract
import openai
from datetime import datetime

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

# 🏗️ Known sources — Construction / AI / Directories / Reviews / Social
KNOWN_SOURCES = {
    # --- Construction / AEC / Industry ---
    "aec-business.com": "AEC Business",
    "constructconnect.com": "ConstructConnect",
    "constructionexec.com": "Construction Executive",
    "constructiondive.com": "Construction Dive",
    "theconstructionindex.co.uk": "The Construction Index",
    "constructionweekonline.com": "Construction Week",
    "constructionglobal.com": "Construction Global",
    "constructech.com": "Constructech",
    "buildingenclosureonline.com": "Building Enclosure",
    "forconstructionpros.com": "For Construction Pros",
    "construction-today.com": "Construction Today",
    "designandbuildreview.com": "Design & Build Review",
    "pbctoday.co.uk": "PBC Today",
    "giatecscientific.com": "Giatec Scientific",
    "constructionplacements.com": "Construction Placements",
    "building.co.uk": "Building UK",
    "bimplus.co.uk": "BIMplus",
    "bimcommunity.com": "BIM Community",
    "bimstore.co.uk": "BIM Store",
    "bimcorner.com": "BIM Corner",
    "geospatialworld.net": "Geospatial World",
    "archdaily.com": "ArchDaily",
    "engineering.com": "Engineering.com",
    "civilplus.com": "Civil Plus",

    # --- AI Tool Directories & SaaS Reviews ---
    "theresanaiforthat.com": "There's an AI For That",
    "futurepedia.io": "Futurepedia",
    "aitoolhunt.com": "AI Tool Hunt",
    "toolify.ai": "Toolify",
    "aibusiness.com": "AI Business",
    "unite.ai": "Unite.AI",
    "aitoptools.com": "AI Top Tools",
    "aitooltracker.com": "AI Tool Tracker",
    "insidr.ai": "Insidr.AI",
    "g2.com": "G2",
    "capterra.com": "Capterra",
    "getapp.com": "GetApp",
    "softwareadvice.com": "Software Advice",
    "saasworthy.com": "SaaSworthy",
    "trustpilot.com": "Trustpilot",
    "producthunt.com": "Product Hunt",
    "betalist.com": "BetaList",
    "angel.co": "AngelList",
    "crunchbase.com": "Crunchbase",
    "appsumo.com": "AppSumo",

    # --- Social Media & Community ---
    "linkedin.com": "LinkedIn",
    "twitter.com": "Twitter",
    "reddit.com": "Reddit",
    "youtube.com": "YouTube",
    "medium.com": "Medium",
    "dev.to": "Dev.to",
    "facebook.com": "Facebook",
    "instagram.com": "Instagram",
    "tiktok.com": "TikTok",
}

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

def classify_source(displayed_link, snippet, title, website_url=None):
    """
    Detects source of the tool based on known domains and avoids marking
    the tool's own website as the source.
    """
    s = (displayed_link or "").lower()
    full_text = (displayed_link or "") + " " + (snippet or "") + " " + (title or "")
    full_text = full_text.lower()

    website_domain = ""
    if website_url:
        website_domain = domain_from_url(website_url)

    for domain, source_name in KNOWN_SOURCES.items():
        if domain in s or domain in full_text:
            if website_domain and domain in website_domain:
                continue
            return source_name

    # Fallback keywords
    if "product hunt" in full_text:
        return "Product Hunt"
    if "linkedin" in full_text:
        return "LinkedIn"
    if "reddit" in full_text:
        return "Reddit"
    if "g2" in full_text:
        return "G2"
    if "construction" in full_text:
        return "Construction News"

    return "Google Search"

def gpt_guess_source(tool_name, description):
    """
    Asks GPT to guess a relevant source for a tool if unknown.
    """
    prompt = f"""
You are a research assistant for Construction AI tools.
A tool is called '{tool_name}' and its description is:
{description}

Guess the most likely source where this tool could be listed or discovered,
choosing from known industry, AI directories, SaaS review platforms, or social media.

Only return the source name (e.g., "G2", "Product Hunt", "Construction Dive", "LinkedIn").
"""
    try:
        resp = openai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        guess = resp.choices[0].message.content.strip()
        return guess
    except:
        return "Google Search"

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
            resp = openai.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role":"user","content": prompt}],
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
You are a strict extractor. INPUT is a JSON array of up to {BATCH_SIZE} search results (title, snippet, link, source).
For each input, extract ONLY if tool_name can be confidently identified from title/snippet.
Output JSON array only with:
tool_name, description, website, source, tags, reviews, launch_date.
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