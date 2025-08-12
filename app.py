import streamlit as st
import pandas as pd
import os
import time
from datetime import datetime
import json
import re
import csv
import mscraper as scraper

OUTPUT_FILE = scraper.OUTPUT_FILE

st.set_page_config(page_title="🏗️ Construction AI Scraper", layout="wide")
st.title("🏗️ Construction AI Tools Scraper")

query = st.text_input("🔍 Search query", value="construction AI tools")
mode = st.radio("📌 Mode", ["Resume", "Start fresh"])
run_button = st.button("🚀 Run Scraper")

status_placeholder = st.empty()

if run_button:
    status_placeholder.write("### ⏳ Running scraper... Please wait.")

    last_offset = scraper.load_last_offset()
    start_offset = last_offset if mode == "Resume" else 0

    scraper.QUERY = query
    scraper.ensure_output_exists()
    seen = scraper.load_seen()

    status_placeholder.write(f"📡 Fetching up to {scraper.RESULTS_PER_RUN} results starting at offset {start_offset}...")
    raw_results = scraper.run_serpapi_pages(start_offset)
    st.write(f"⚙️ Collected **{len(raw_results)}** raw SERP candidates.")

    unique = []
    seen_keys = set()
    for r in raw_results:
        key = (r.get("link") or "") + "||" + (r.get("title") or "")
        if key not in seen_keys:
            seen_keys.add(key)
            unique.append(r)
    st.write(f"⚙️ {len(unique)} unique candidates after dedupe.")

    candidates = []
    for item in unique:
        src = scraper.classify_source(item.get("displayed_link", ""), item.get("snippet", ""), item.get("title", ""), item.get("link", ""))
        candidates.append({
            "title": item.get("title", ""),
            "snippet": item.get("snippet", ""),
            "link": item.get("link", ""),
            "source": src
        })

    filtered = []
    for c in candidates:
        naive_name = (c["title"] or "").split("—")[0].split("|")[0].strip()
        if naive_name and naive_name.lower() in (s.lower() for s in seen):
            continue
        filtered.append(c)
    st.write(f"⚙️ {len(filtered)} candidates passed seen check.")

    total_saved = 0
    batch_num = 0
    progress_bar = st.progress(0)

    for i in range(0, len(filtered), scraper.BATCH_SIZE):
        batch = filtered[i:i+scraper.BATCH_SIZE]
        batch_num += 1
        status_placeholder.write(f"⚙️ Sending batch {batch_num} ({i+1}-{i+len(batch)}) to GPT...")
        raw = scraper.safe_gpt_call(scraper.build_prompt(batch, batch_num), max_retries=5)

        if not raw:
            continue
        json_text = scraper.clean_json_from_gpt(raw)
        if not json_text:
            continue
        try:
            parsed = json.loads(json_text)
            if not isinstance(parsed, list):
                continue
        except:
            continue

        written = 0
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for obj in parsed:
                if not isinstance(obj, dict):
                    continue
                tn = str(obj.get("tool_name", "")).strip()
                desc = str(obj.get("description", "")).strip()
                web = str(obj.get("website", "")).strip()
                src = str(obj.get("source", "")).strip()
                tags = str(obj.get("tags", "")).strip()
                reviews = str(obj.get("reviews", "")).strip()
                launch = str(obj.get("launch_date", "")).strip()

                if not tn or not desc or not web or not src:
                    continue
                if not re.match(r"^\d+$", reviews):
                    reviews = scraper.extract_review_count(desc)
                    if not reviews:
                        reviews = "0"
                if not tags:
                    tags = "AI, construction"
                if tn.lower() in (s.lower() for s in seen):
                    continue

                w.writerow([tn, desc, web, src, tags, reviews, launch])
                seen.add(tn)
                written += 1
                total_saved += 1

        progress_bar.progress(min((i+scraper.BATCH_SIZE)/len(filtered), 1.0))
        time.sleep(1.2)

    new_offset = start_offset + (scraper.PAGES_PER_RUN * scraper.RESULTS_PER_PAGE)
    scraper.save_last_offset(new_offset)
    scraper.save_seen(seen)

    status_placeholder.success(f"✅ Done! {total_saved} new tools saved.")

if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
    try:
        df = pd.read_csv(OUTPUT_FILE)
        st.write(f"### 📊 Current scraped tools ({len(df)})")
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error reading CSV: {e}")
else:
    st.info("No scraped data yet. Run the scraper to see results.")