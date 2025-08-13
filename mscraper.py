# mscraper.py
# Multi-engine scraper: GPT extracts core tool data, Grok enriches with metadata.
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

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

RESULTS_PER_RUN   = 100
PAGES_PER_RUN     = 10
RESULTS_PER_PAGE  = 10
BATCH_SIZE        = 10

OUTPUT_FILE       = "construction_tools.csv"
SEEN_FILE         = "seen_tools.csv"
LAST_OFFSET_FILE  = "last_offset.txt"
RATE_LIMIT_BACKOFF = 8

QUERY = "construction AI tools"

# ---------- Reputable sources ----------
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
    "Futurepedia":       ["futurepedia.io"],
    "There's an AI for That": ["theresanaiforthat.com"],
    "AlternativeTo":     ["alternativeto.net"],
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

REPUTABLE_DOMAIN_SET = set(d for ds in REPUTABLE_SOURCES.values() for d in ds)

# ---------- Construction relevance ----------
CONSTRUCTION_KEYWORDS = {
    "construction","builder","building","architect","architecture","architectural",
    "civil engineering","structural","mep","bim","cad","revit","autocad","navisworks","ifc",
    "construction management","jobsite","quantity takeoff","estimation",
    "scheduling","clash detection","rfis","submittals","punch list",
    "plan","blueprint","floor plan","as-built","contractor","subcontractor",
    "project delivery","site safety","prefab","lean construction","VDC","AEC","tender","bid"
}
CONSTRUCTION_DOMAIN_HINTS = {"bim","cad","revit","autocad","navisworks","aeco","aec"}

def contains_any(text: str, terms: set) -> bool:
    if not text: return False
    t = text.lower()
    return any(k in t for k in terms)

def looks_construction_related(name, desc, tags, website):
    bag = " ".join([(name or ""), (desc or ""), (tags or ""), (website or "")]).lower()
    if contains_any(bag, CONSTRUCTION_KEYWORDS): return True
    if contains_any(website or "", CONSTRUCTION_DOMAIN_HINTS): return True
    if isinstance(tags, str) and "construction" in tags.lower(): return True
    return False

def enforce_ai_construction_tags(tags_str):
    tags = []
    if isinstance(tags_str, list):
        tags = [str(x).strip() for x in tags_str if str(x).strip()]
    elif isinstance(tags_str, str):
        tags = [t.strip() for t in tags_str.split(",") if t.strip()]
    else:
        tags = [str(tags_str).strip()] if tags_str else []
    base = [t.lower() for t in tags]
    if "ai" not in base: tags.insert(0, "AI")
    if "construction" not in base: tags.insert(1, "construction")
    seen, out = set(), []
    for t in tags:
        tl = t.lower()
        if tl not in seen:
            seen.add(tl)
            out.append(t)
    return ", ".join(out[:6])

# ---------- Utils ----------
def ensure_output_exists():
    if not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["tool_name","description","website","source","tags","reviews","launch_date"])

def load_seen():
    return {line.strip().lower() for line in open(SEEN_FILE, "r", encoding="utf-8")} if os.path.exists(SEEN_FILE) else set()

def save_seen(seen_set):
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        for s in sorted(seen_set): f.write(s + "\n")

def load_last_offset():
    try:
        return int(open(LAST_OFFSET_FILE).read().strip())
    except: return 0

def save_last_offset(offset):
    open(LAST_OFFSET_FILE, "w").write(str(int(offset)))

def domain_from_url(url):
    if not url: return ""
    try:
        ex = tldextract.extract(url)
        if ex.domain:
            return (ex.domain + (("." + ex.suffix) if ex.suffix else "")).lower()
    except: pass
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except: return ""

def safe_get_str(obj, key):
    if not isinstance(obj, dict): return ""
    val = obj.get(key, "")
    if isinstance(val, str): return val.strip()
    if isinstance(val, list): return " ".join(str(x) for x in val)
    if isinstance(val, dict): return json.dumps(val, ensure_ascii=False)
    return str(val).strip()

# ---------- Engines ----------
def fetch_serpapi(start_offset):
    if not SERP_API_KEY: return []
    out = []
    for page in range(PAGES_PER_RUN):
        offset = start_offset + page * RESULTS_PER_PAGE
        params = {"engine":"google","q":QUERY,"start":offset,"num":RESULTS_PER_PAGE,"api_key":SERP_API_KEY}
        try:
            r = requests.get("https://serpapi.com/search.json", params=params, timeout=25)
            hits = r.json().get("organic_results") or []
            for h in hits:
                out.append({"title":h.get("title",""),"snippet":h.get("snippet",""),"link":h.get("link",""),"displayed_link":h.get("displayed_link",""),"engine":"serpapi"})
            time.sleep(0.6)
        except: pass
    return out

def fetch_google_cse(start_offset):
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID: return []
    out = []
    for page in range(PAGES_PER_RUN):
        start = start_offset + page * RESULTS_PER_PAGE + 1
        params = {"key":GOOGLE_API_KEY,"cx":GOOGLE_CSE_ID,"q":QUERY,"start":start,"num":RESULTS_PER_PAGE}
        try:
            r = requests.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=25)
            items = r.json().get("items", [])
            for it in items:
                out.append({"title":it.get("title",""),"snippet":it.get("snippet",""),"link":it.get("link",""),"displayed_link":it.get("displayLink",""),"engine":"google_cse"})
            time.sleep(0.6)
        except: pass
    return out

def fetch_bing(start_offset):
    if not BING_API_KEY: return []
    out = []
    headers = {"Ocp-Apim-Subscription-Key": BING_API_KEY}
    for page in range(PAGES_PER_RUN):
        offset = start_offset + page * RESULTS_PER_PAGE
        params = {"q":QUERY,"count":RESULTS_PER_PAGE,"offset":offset}
        try:
            r = requests.get("https://api.bing.microsoft.com/v7.0/search", headers=headers, params=params, timeout=25)
            webPages = r.json().get("webPages", {}).get("value", [])
            for w in webPages:
                out.append({"title":w.get("name",""),"snippet":w.get("snippet",""),"link":w.get("url",""),"displayed_link":domain_from_url(w.get("url","")),"engine":"bing"})
            time.sleep(0.6)
        except: pass
    return out

def aggregate_results(start_offset):
    bag = fetch_serpapi(start_offset) + fetch_google_cse(start_offset) + fetch_bing(start_offset)
    seen, dedup = set(), []
    for r in bag:
        key = (r.get("link") or "") + "||" + (r.get("title") or "")
        if key not in seen:
            seen.add(key)
            dedup.append(r)
    return dedup

# ---------- AI helpers ----------
_openai_client = None
def get_openai_client():
    global _openai_client
    if _openai_client or not OPENAI_API_KEY: return _openai_client
    from openai import OpenAI
    _openai_client = OpenAI(api_key=OPENAI_API_KEY)
    return _openai_client

def safe_gpt_call(prompt):
    client = get_openai_client()
    try:
        resp = client.chat.completions.create(model=OPENAI_MODEL, messages=[{"role":"user","content": prompt}], temperature=0)
        return resp.choices[0].message.content
    except: return None

def grok_complete(prompt):
    if not GROK_API_KEY: return None
    url = "https://api.x.ai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROK_API_KEY}","Content-Type": "application/json"}
    payload = {"model":"grok-2-latest","messages":[{"role":"user","content":prompt}],"temperature":0}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=40)
        return r.json().get("choices", [{}])[0].get("message", {}).get("content", "")
    except: return None

# ---------- Prompts ----------
def build_gpt_prompt(batch):
    return f"""
Extract only construction/AEC/BIM relevant tools.
Return JSON array with: tool_name, description (8–30 words), website (domain only).
Skip if relevance unclear.
INPUT: {json.dumps(batch, ensure_ascii=False)}
"""

def build_grok_prompt(item):
    preferred = ", ".join(sorted(REPUTABLE_DOMAIN_SET))
    return f"""
Enrich the tool below with: source (≠ website domain, prefer: {preferred}), tags (include 'AI' & 'construction' + 2–3 others), reviews (integer), launch_date.
If no clear source, use Google search URL: https://www.google.com/search?q=<tool name>%20Product%20Hunt%20G2
Return JSON object with these exact keys: source, tags, reviews, launch_date.
Tool: {json.dumps(item, ensure_ascii=False)}
"""

# ---------- Main ----------
def run_scrape(query, mode="Resume"):
    global QUERY
    QUERY = query or QUERY
    ensure_output_exists()
    seen_names = load_seen()
    last_offset = load_last_offset()
    start_offset = last_offset if mode.lower().startswith("resume") else 0

    print(f"🔍 Searching from offset {start_offset}")
    results = aggregate_results(start_offset)
    print(f"📊 Total search results: {len(results)}")

    total_saved, batch_num = 0, 0
    for i in range(0, len(results), BATCH_SIZE):
        batch_num += 1
        batch = results[i:i+BATCH_SIZE]
        print(f"⚙️ Batch {batch_num}: sending to GPT for core fields...")
        raw_gpt = safe_gpt_call(build_gpt_prompt(batch))
        try:
            gpt_items = json.loads(re.search(r"(\[.*\])", raw_gpt, re.DOTALL).group(1))
        except: gpt_items = []

        # Filter relevance
        gpt_items = [it for it in gpt_items if looks_construction_related(it.get("tool_name",""), it.get("description",""), "", it.get("website",""))]

        enriched_items = []
        for it in gpt_items:
            print(f"   🧠 Grok enriching: {it.get('tool_name')}")
            raw_grok = grok_complete(build_grok_prompt(it))
            try:
                grok_data = json.loads(re.search(r"(\{.*\})", raw_grok, re.DOTALL).group(1))
            except: grok_data = {"source":"","tags":"","reviews":"0","launch_date":""}
            grok_data["tags"] = enforce_ai_construction_tags(grok_data.get("tags",""))
            enriched_items.append({**it, **grok_data})

        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for obj in enriched_items:
                tn, web = obj["tool_name"], domain_from_url(obj["website"])
                if tn.lower() in seen_names: continue
                seen_names.add(tn.lower())
                w.writerow([obj["tool_name"], obj["description"], web, obj["source"], obj["tags"], obj["reviews"], obj["launch_date"]])
                total_saved += 1

    save_seen(seen_names)
    save_last_offset(start_offset + RESULTS_PER_RUN)
    print(f"✅ Saved {total_saved} new tools.")
    return total_saved, start_offset + RESULTS_PER_RUN, enriched_items