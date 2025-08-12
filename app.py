# app.py (Streamlit) - Updated for cleaner messaging & batch progress
import streamlit as st
import pandas as pd
import os
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
    status_placeholder.info("⏳ Running scraper — please wait.")
    start_offset = scraper.load_last_offset() if mode == "Resume" else 0

    scraper.ensure_output_exists()
    seen = scraper.load_seen()

    status_placeholder.write(f"📡 Fetching up to {scraper.RESULTS_PER_RUN} results starting at offset {start_offset}...")
    raw_results = scraper.run_serpapi_pages(start_offset, query)
    st.write(f"⚙️ Collected **{len(raw_results)}** raw candidates.")

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

    # Prepare candidates
    candidates = []
    for item in unique:
        website_domain = scraper.domain_from_url(item.get("link", ""))
        candidates.append({
            "title": item.get("title", ""),
            "snippet": item.get("snippet", ""),
            "link": item.get("link", ""),
            "source": scraper.classify_source(item.get("displayed_link", ""), item.get("snippet", ""), item.get("title", ""), website_domain)
        })

    st.write(f"📤 Sending {len(candidates)} candidates to GPT in batches of {scraper.BATCH_SIZE}...")

    # Process in batches and show progress
    total_saved = 0
    total_batches = (len(candidates) + scraper.BATCH_SIZE - 1) // scraper.BATCH_SIZE
    for i in range(0, len(candidates), scraper.BATCH_SIZE):
        batch_num = (i // scraper.BATCH_SIZE) + 1
        st.write(f"🔄 Processing batch {batch_num} of {total_batches}...")
        batch = candidates[i:i + scraper.BATCH_SIZE]
        saved_count = scraper.process_batch(batch, query)  # New helper in mscraper
        total_saved += saved_count
        progress_bar.progress(batch_num / total_batches)

    scraper.save_last_offset(start_offset + scraper.RESULTS_PER_RUN)
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