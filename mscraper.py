# mscraper.py
# Multi-engine scraper + GPT extraction + Grok enrichment
# CSV columns: tool_name, description, website, source, tags, reviews, launch_date

import os, time, json, csv, re
from urllib.parse import urlparse, urlencode
import requests
import tldextract
from datetime import datetime

# ---------- CONFIG ----------
SERP_API_KEY    = os.getenv("SERP_API_KEY", "")
GOOGLE_API_KEY  = os.getenv("GOOGLE_API_KEY", "")
GOOGLE_CSE_ID   = os.getenv("GOOGLE_CSE_ID", "")
SERPER_API_KEY  = os.getenv("SERPER_API_KEY", "")

OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL    = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # OpenAI v1 model
GROK_API_KEY    = os.getenv("GROK_API_KEY", "")

# Scraping params (≈100 results per engine per run; dedupe before GPT)
RESULTS_PER_RUN    = 100
PAGES_PER_RUN      = 10
RESULTS_PER_PAGE   = 10
BATCH_SIZE         = 10
ENGINE_TIMEOUT     = 25

OUTPUT_FILE        = "construction_tools.csv"
SEEN_FILE          = "seen_tools.csv"
LAST_OFFSET_FILE   = "last_offset.txt"
RATE_LIMIT_BACKOFF = 8

# Default query (app.py can overwrite mscraper.QUERY)
QUERY = "construction AI tools"

# ---------- Reputable domains we prefer for "source" ----------
REPUTABLE_SOURCES = {
    "Product Hunt":      ["producthunt.com"],
    "G2":                ["g2.com"],
    "Capterra":          ["capterra.com"],
    "GetApp":            ["getapp.com"],
    "AlternativeTo":     ["alternativeto.net"],
    "Futurepedia":       ["futurepedia.io"],
    "ThereIsAnAIForThat":["theresanaiforthat.com"],
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
    "TechCrunch":        ["techcrunch.com"],
    "The Verge":         ["theverge.com"],
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

# ---------- Utilities ----------
def ensure_output_exists():
    """Create CSV with 7 headers if missing or empty."""
    need_header = (not os.path.exists(OUTPUT_FILE)) or (os.path.getsize(OUTPUT_FILE) == 0)
    if need_header:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["tool_name","description","website","source","tags","reviews","launch_date"])

def load_seen():
    s = set()
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            for line in f:
                v = line.strip().lower()
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
        netloc = urlparse(url).netloc.lower().replace("www.", "")
        return netloc
    except:
        return ""

def safe_get_str(obj, key):
    """Safely get a string for a dict field; join lists/dicts into string when needed."""
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

def construction_related_score(text):
    """Simple heuristic to check construction relevance."""
    if not text:
        return 0
    text = text.lower()
    keywords = [
        "construction","builder","contractor","jobsite","bim","aec","architecture",
        "architectural","engineering","civil engineering","site","project delivery",
        "estimating","takeoff","quantity takeoff","scheduling","rfis","submittals",
        "field","safety","punchlist","as-built","prefab","revit","navisworks"
    ]
    score = sum(1 for k in keywords if k in text)
    return score

# ---------- Engines ----------
def fetch_serpapi(start_offset):
    if not SERP_API_KEY:
        print("ℹ️ SerpAPI key not set — skipping SerpAPI.")
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
            r = requests.get(SERPAPI_URL, params=params, timeout=ENGINE_TIMEOUT)
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
            time.sleep(1.2)
            continue
    print(f"🔍 SerpAPI fetched {len(results)} items.")
    return results

def fetch_google_cse(start_offset):
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        print("ℹ️ Google CSE key or CX missing — skipping Google CSE.")
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
            r = requests.get(base, params=params, timeout=ENGINE_TIMEOUT)
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
            time.sleep(1.2)
            continue
    print(f"🔍 Google CSE fetched {len(results)} items.")
    return results

def fetch_serper(start_offset):
    if not SERPER_API_KEY:
        print("ℹ️ Serper.dev API key missing — skipping Serper.dev.")
        return []
    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    results = []
    # serper supports "page" and "num"
    for page in range(1, PAGES_PER_RUN + 1):
        payload = {
            "q": QUERY,
            "num": RESULTS_PER_PAGE,
            "page": page
        }
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=ENGINE_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            hits = data.get("organic", [])
            for h in hits:
                results.append({
                    "title": (h.get("title") or "").strip(),
                    "snippet": (h.get("snippet") or "").strip(),
                    "link": (h.get("link") or h.get("url") or "").strip(),
                    "displayed_link": domain_from_url(h.get("link") or h.get("url") or ""),
                    "engine": "serper"
                })
            time.sleep(0.6)
        except Exception as e:
            print(f"❌ Serper.dev failed @page={page}: {e}")
            time.sleep(1.2)
            continue
    print(f"🔍 Serper.dev fetched {len(results)} items.")
    return results

def aggregate_results(start_offset):
    """Collect from all available engines; dedupe by link+title."""
    print("🔎 Running multi-engine search...")
    bag = []
    g = fetch_google_cse(start_offset)
    print(f"   → First search (Google CSE): {len(g)}")
    bag.extend(g)

    s = fetch_serpapi(start_offset)
    print(f"   → Second search (SerpAPI):   {len(s)}")
    bag.extend(s)

    p = fetch_serper(start_offset)
    print(f"   → Third search (Serper.dev): {len(p)}")
    bag.extend(p)

    dedup = []
    seen = set()
    for r in bag:
        key = (r.get("link") or "") + "||" + (r.get("title") or "")
        if key in seen:
            continue
        seen.add(key)
        dedup.append(r)
    print(f"📊 Total unique results after dedupe: {len(dedup)}")
    return dedup

# ---------- OpenAI (v1) + Grok ----------
_openai_client = None
def get_openai_client():
    global _openai_client
    if _openai_client is not None:
        return _openai_client
    if not OPENAI_API_KEY:
        print("⚠️ OPENAI_API_KEY missing — GPT extractor disabled.")
        return None
    try:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=OPENAI_API_KEY)
        return _openai_client
    except Exception as e:
        print("⚠️ OpenAI v1 client not available:", e)
        return None

def safe_gpt_call(prompt, max_retries=5, temperature=0):
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
def build_extractor_prompt(batch, batch_num):
    """
    GPT Extractor: ONLY extract tool_name, description (8–30 words),
    website (domain only) for construction/AEC-related tools.
    """
    payload = json.dumps(batch, ensure_ascii=False)
    return f"""
You are extracting tools from search results. INPUT: JSON array (up to {BATCH_SIZE}) with fields:
title, snippet, link, displayed_link, engine.

Only extract an item if it is clearly a tool/product relevant to Construction / AEC / Architecture / Engineering / BIM / jobsite workflows.
Do NOT invent tool names. If the item is not clearly a tool used by construction professionals, skip it.

Return STRICT JSON ARRAY (no commentary). Each object:
- tool_name (string) — from title/snippet, canonical product name only
- description (string, 8–30 words) — concise summary relevant to construction usage
- website (domain only, e.g., "togal.ai") — official domain if clear; else domain_from_url(link)

Rules:
- JSON ARRAY only, no extra text.
- If tool_name is not clear or not construction-related, skip.
- description should reflect construction/AEC usage.
- No extra fields.

Batch {batch_num}
INPUT:
{payload}
""".strip()

def build_grok_enricher_prompt(item, candidates_str):
    """
    Grok Enricher: given tool_name + website + description, produce source, tags, reviews, launch_date.
    Must ensure source domain != website domain. Prefer reputable domains.
    """
    name = safe_get_str(item, "tool_name")
    website = safe_get_str(item, "website")
    desc = safe_get_str(item, "description")

    preferred = ", ".join(sorted(REPUTABLE_DOMAIN_SET))
    g_query = f"{name} reviews producthunt g2 capterra futurepedia alternativeto"
    g_url = make_google_query_url(g_query)

    return f"""
You are enriching a construction tool with reputable source + tags + reviews + launch date.

Tool:
- name: {name}
- website domain: {website}
- description: {desc}

Candidate URLs from the scrape (some may be relevant):
{candidates_str}

Requirements:
- Return STRICT JSON OBJECT with keys: source, tags, reviews, launch_date.
- source: a reputable third-party URL about the tool (NOT the same domain as website).
  Prefer domains among: {preferred}
  Good: ProductHunt, G2, Capterra, GetApp, AlternativeTo, Futurepedia, Crunchbase, AngelList/Wellfound, GitHub, Reddit, LinkedIn, Medium, HN, YouTube.
  If none are known, return a Google search URL like: {g_url}
- tags: must include "AI" and "construction", plus 2–3 additional concise tags (comma-separated).
- reviews: an integer string if any review/user count is known; else "0".
- launch_date: year or month-year if known; else "".

Return JSON ONLY (no commentary).
""".strip()

# ---------- Parsing & normalization ----------
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

def normalize_extracted(obj):
    tn = safe_get_str(obj, "tool_name")
    desc = safe_get_str(obj, "description")
    web = safe_get_str(obj, "website")

    # Ensure website is domain only
    web_domain = domain_from_url(web) if "." in web else web
    return {
        "tool_name": tn,
        "description": desc,
        "website": web_domain
    }

def build_candidates_index(batch):
    """
    Build a small index of candidate URLs per batch to help Grok choose a reputable source.
    """
    lines = []
    for b in batch:
        t = safe_get_str(b, "title")
        u = safe_get_str(b, "link")
        d = domain_from_url(u)
        if t and u:
            lines.append(f"- {t} → {u} ({d})")
    return "\n".join(lines) if lines else "- (no candidates)"

def suggest_source_from_batch(tool_name, website, batch):
    """
    Heuristic: if batch contains a reputable URL mentioning the tool name
    and not same-domain as website, pick it as source immediately.
    """
    name_l = (tool_name or "").lower()
    web_d = domain_from_url(website)
    best = None
    for b in batch:
        url = safe_get_str(b, "link")
        dom = domain_from_url(url)
        t = safe_get_str(b, "title").lower()
        sn = safe_get_str(b, "snippet").lower()
        if not url or not dom: 
            continue
        if dom == web_d:
            continue
        if looks_reputable(dom):
            # require mention or tool name similarity in title/snippet
            if name_l and (name_l in t or name_l in sn):
                best = url
                break
    return best

# ---------- Main scrape ----------
def run_scrape(query, mode="Resume"):
    """
    Returns (total_saved, last_offset, parsed_items) and writes CSV lines.
    Uses three engines (where keys exist), dedupes, GPT-extracts (name/desc/site),
    then Grok enriches (source/tags/reviews/launch_date).
    """
    global QUERY
    QUERY = query or QUERY

    ensure_output_exists()
    seen_names = load_seen()
    last_offset = load_last_offset()
    start_offset = last_offset if mode.lower().startswith("resume") else 0

    print(f"🔍 Fetching up to {RESULTS_PER_RUN} results starting @offset {start_offset}...")
    raw_results = aggregate_results(start_offset)
    print(f"⚙️ Aggregated {len(raw_results)} raw candidates.")

    # Lightweight filter to push construction-related content up
    # (We do not hard-drop, GPT will decide, but sorting helps)
    scored = []
    for r in raw_results:
        score = construction_related_score((r.get("title","") + " " + r.get("snippet","")))
        scored.append((score, r))
    # sort descending by construction relevance
    scored.sort(key=lambda x: x[0], reverse=True)
    candidates = [r for _, r in scored]

    total_saved = 0
    batch_num = 0
    all_parsed = []

    for i in range(0, len(candidates), BATCH_SIZE):
        batch = candidates[i:i+BATCH_SIZE]
        batch_num += 1
        print(f"⚙️ Sending Batch {batch_num} ({i+1}-{i+len(batch)}) → GPT Extractor")

        # GPT: extract tool_name, description, website
        gpt_prompt = build_extractor_prompt(batch, batch_num)
        raw = safe_gpt_call(gpt_prompt, max_retries=5, temperature=0)
        parsed = parse_gpt_json(raw)
        if not parsed:
            print("⚠️ GPT returned no valid JSON for this batch.")
            continue

        # Normalize extracted items
        extracted = []
        for p in parsed:
            if not isinstance(p, dict):
                continue
            item = normalize_extracted(p)
            tn = item["tool_name"]
            desc = item["description"]
            web = item["website"]
            if not (tn and desc and web):
                continue
            # dedupe by tool_name lowercase
            if tn.lower() in seen_names:
                continue
            extracted.append(item)

        if not extracted:
            print("ℹ️ Extractor produced 0 usable items in this batch.")
            continue

        # Try to assign a reputable 'source' heuristically from the batch URLs, then ask Grok if needed
        candidates_str = build_candidates_index(batch)
        enriched = []
        for it in extracted:
            # Heuristic: choose source from batch first
            heuristic_src = suggest_source_from_batch(it["tool_name"], it["website"], batch)
            enriched_item = {
                "tool_name": it["tool_name"],
                "description": it["description"],
                "website": it["website"],
                "source": heuristic_src or "",
                "tags": "AI, construction",
                "reviews": "0",
                "launch_date": ""
            }
            enriched.append(enriched_item)

        # Grok enrich only items that still need better data (source missing or reviews 0 or launch_date empty)
        need_grok = []
        for en in enriched:
            needs = (not en["source"]) or (en["reviews"] == "0") or (not en["launch_date"]) or ("AI" not in en["tags"] or "construction" not in en["tags"])
            if needs:
                need_grok.append(en)

        if need_grok and GROK_API_KEY:
            print(f"⚙️ Sending {len(need_grok)} items → Grok Enricher")
        elif need_grok and not GROK_API_KEY:
            print("ℹ️ GROK_API_KEY missing — skipping Grok enrichment.")

        for en in need_grok:
            # Build prompt per item for reliability
            gp = build_grok_enricher_prompt(en, candidates_str)
            out = grok_complete(gp, max_retries=4, temperature=0)
            data = parse_gpt_json(out)
            if data and isinstance(data[0], dict):
                da = data[0]
                # source
                src = safe_get_str(da, "source")
                if src:
                    # ensure not same-domain as website
                    if domain_from_url(src) == domain_from_url(en["website"]):
                        # fallback to Google search for reputable queries
                        src = make_google_query_url(f'{en["tool_name"]} reviews producthunt g2 capterra futurepedia alternativeto')
                    en["source"] = src
                # tags – ensure includes AI and construction
                tags = safe_get_str(da, "tags")
                if tags:
                    # Normalize tags and enforce 'AI' and 'construction'
                    tag_list = [t.strip() for t in tags.split(",") if t.strip()]
                    low = [t.lower() for t in tag_list]
                    if "ai" not in low:
                        tag_list.insert(0, "AI")
                    if "construction" not in low:
                        tag_list.insert(1, "construction")
                    en["tags"] = ", ".join(tag_list[:5])  # at most 5 tags
                # reviews
                reviews = safe_get_str(da, "reviews")
                if not re.match(r"^\d+$", reviews or ""):
                    # try to guess from description
                    reviews = extract_review_count(en["description"]) or "0"
                en["reviews"] = reviews
                # launch_date
                ld = safe_get_str(da, "launch_date")
                en["launch_date"] = ld

        # Final safety for source: if missing or same-domain, use Google search
        for en in enriched:
            if (not en["source"]) or (domain_from_url(en["source"]) == domain_from_url(en["website"])):
                en["source"] = make_google_query_url(f'{en["tool_name"]} reviews producthunt g2 capterra futurepedia alternativeto')

            # Guarantee tags include 'AI' and 'construction'
            tags = en.get("tags", "")
            tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
            lower = [t.lower() for t in tag_list]
            changed = False
            if "ai" not in lower:
                tag_list.insert(0, "AI"); changed=True
            if "construction" not in lower:
                tag_list.insert(1, "construction"); changed=True
            if changed:
                en["tags"] = ", ".join(tag_list[:5]) if tag_list else "AI, construction"

            # reviews digits-only
            if not re.match(r"^\d+$", en.get("reviews","")):
                en["reviews"] = extract_review_count(en.get("description","")) or "0"

        # Write to CSV, dedupe by tool_name lower
        written = 0
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for obj in enriched:
                tn   = safe_get_str(obj, "tool_name")
                desc = safe_get_str(obj, "description")
                web  = safe_get_str(obj, "website")
                src  = safe_get_str(obj, "source")
                tags = safe_get_str(obj, "tags")
                rev  = safe_get_str(obj, "reviews")
                ld   = safe_get_str(obj, "launch_date")

                if not (tn and desc and web and src):
                    continue
                if tn.lower() in seen_names:
                    continue

                # Ensure source and website are not same domain
                if domain_from_url(src) == domain_from_url(web):
                    src = make_google_query_url(f"{tn} reviews producthunt g2 capterra futurepedia alternativeto")

                # 7 columns only
                w.writerow([tn, desc, web, src, tags, rev, ld])
                seen_names.add(tn.lower())
                written += 1
                total_saved += 1

        print(f"✅ Batch {batch_num}: saved {written} new tools.")

        # Be nice to APIs
        time.sleep(1.2)

    # Update offsets & persist seen
    new_offset = start_offset + (PAGES_PER_RUN * RESULTS_PER_PAGE)
    save_last_offset(new_offset)
    save_seen(seen_names)

    print(f"🎯 Done. Total new tools saved this run: {total_saved}. Last offset → {new_offset}")
    return total_saved, new_offset, []