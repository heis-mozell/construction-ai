# app.py (Streamlit)
import streamlit as st
import pandas as pd
import os
import time
import json
import re
import csv
from datetime import datetime
import mscraper as scraper

OUTPUT_FILE = scraper.OUTPUT_FILE

st.set_page_config(page_title="🏗️ Construction AI Scraper", layout="wide")
st.title("🏗️ Construction AI Tools Scraper")

query = st.text_input("🔍 Search query", value="construction AI tools")
mode = st.radio("📌 Mode", ["Resume", "Start fresh"])
run_button = st.button("🚀 Run Scraper")

status_placeholder = st.empty()
progress_bar = st.progress(0)

if run_button:
    status_placeholder.info("⏳ Running scraper — please wait. This can take a minute or two.")
    start_offset = scraper.load_last_offset() if mode == "Resume" else 0

    scraper.ensure_output_exists()
    seen = scraper.load_seen()

    status_placeholder.write(f"📡 Fetching up to {scraper.RESULTS_PER_RUN} results starting at offset {start_offset}...")
    raw_results = scraper.run_serpapi_pages(start_offset, query)
    st.write(f"⚙️ Collected **{len(raw_results)}** raw SERP candidates.")

    # Dedup by title+link
    unique = []
    seen_keys = set()
    for r in raw_results:
        key = (r.get("link") or "") + "||" + (r.get("title") or "")
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique.append(r)
    st.write(f"⚙️ {len(unique)} unique candidates after initial dedupe.")

    # Prepare candidates (pass fallback website domain so source classifier avoids using same domain)
    candidates = []
    for item in unique:
        candidates.append({
            "title": item.get("title", ""),
            "snippet": item.get("snippet", ""),
            "link": item.get("link", ""),
            "source": scraper.classify_source(item.get("displayed_link",""), item.get("snippet",""), item.get("title",""), fallback_domain=scraper.domain_from_url(item.get("link","")))
        })

    # naive seen removal
    filtered = []
    seen_lower = set(s.lower() for s in seen)
    for c in candidates:
        naive_name = (c["title"] or "").split("—")[0].split("|")[0].strip()
        if naive_name and naive_name.lower() in seen_lower:
            continue
        filtered.append(c)
    st.write(f"⚙️ {len(filtered)} candidates passed naive seen check.")

    # Batch processing
    total_saved = scraper.run_once(query, resume=(mode == "Resume"), verbose=True)
    status_placeholder.success(f"✅ Done! {total_saved} new tools saved. Last offset updated.")

# Always attempt to show the CSV (7 columns)
if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
    try:
        df = pd.read_csv(OUTPUT_FILE, names=["tool_name", "description", "website", "source", "tags", "reviews", "launch_date"])
        st.write(f"### 📊 Current scraped tools ({len(df)})")
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error reading CSV: {e}")
else:
    st.info("No scraped data yet. Run the scraper to see results.")