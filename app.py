import streamlit as st
import pandas as pd
import os
import time
from datetime import datetime
import json
import re
import mscraper as scraper  # Import all your existing functions

OUTPUT_FILE = scraper.OUTPUT_FILE

st.set_page_config(page_title="🏗️ Construction AI Scraper", layout="wide")
st.title("🏗️ Construction AI Tools Scraper")

# Input UI
query = st.text_input("🔍 Search query", value="construction AI tools")
mode = st.radio("📌 Mode", ["Resume", "Start fresh"])
run_button = st.button("🚀 Run Scraper")

# Progress placeholder
status_placeholder = st.empty()

# Run scraper when button clicked
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

    # Deduplication
    unique = []
    seen_keys = set()
    for r in raw_results:
        key = (r.get("link") or "") + "||" + (r.get("title") or "")
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique.append(r)
    st.write(f"⚙️ {len(unique)} unique candidates after initial dedupe.")

    # Prepare candidates
    candidates = []
    for item in unique:
        src = scraper.classify_source(item.get("displayed_link",""), item.get("snippet",""), item.get("title",""))
        candidates.append({
            "title": item.get("title",""),
            "snippet": item.get("snippet",""),
            "link": item.get("link",""),
            "source": src
        })

    # Remove already-seen tools
    filtered = []
    for c in candidates:
        naive_name = (c["title"] or "").split("—")[0].split("|")[0].strip()
        if naive_name and naive_name.lower() in (s.lower() for s in seen):
            continue
        filtered.append(c)
    st.write(f"⚙️ {len(filtered)} candidates passed naive seen check.")

    # Process in batches with GPT
    total_saved = 0
    batch_num = 0
    progress_bar = st.progress(0)
    for i in range(0, len(filtered), scraper.BATCH_SIZE):
        batch = filtered[i:i+scraper.BATCH_SIZE]
        batch_num += 1
        status_placeholder.write(f"⚙️ Sending batch {batch_num} ({i+1}-{i+len(batch)}) to GPT...")
        prompt = scraper.build_prompt(batch, batch_num)
        raw = scraper.safe_gpt_call(prompt, max_retries=5)

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

        # Write validated rows
        written = 0
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            import csv
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
                    continue
                if not re.match(r"^\d+$", reviews or ""):
                    reviews = scraper.extract_review_count(desc + " " + (batch[0].get("snippet","") if batch else ""))
                    if not reviews:
                        reviews = "0"
                if not tags:
                    tags = "AI, construction"
                if tn.lower() in (s.lower() for s in seen):
                    continue
                w.writerow([tn, desc, web, src, tags, reviews, launch, datetime.utcnow().isoformat()])
                seen.add(tn)
                written += 1
                total_saved += 1

        progress_bar.progress(min((i+scraper.BATCH_SIZE)/len(filtered), 1.0))
        time.sleep(1.2)

    # Save updated state
    new_offset = start_offset + (scraper.PAGES_PER_RUN * scraper.RESULTS_PER_PAGE)
    scraper.save_last_offset(new_offset)
    scraper.save_seen(seen)

    status_placeholder.success(f"✅ Done! {total_saved} new tools saved. Last offset: {new_offset}")

# Always show the CSV data if available
if os.path.exists(OUTPUT_FILE):
    df = pd.read_csv(OUTPUT_FILE)
    st.write(f"### 📊 Current scraped tools ({len(df)})")
    st.dataframe(df)
else:
    st.info("No scraped data yet. Run the scraper to see results.")