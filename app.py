import streamlit as st
import pandas as pd
import os, time, json, csv, re
from datetime import datetime

import mscraper as scraper  # use functions from mscraper

st.set_page_config(page_title="🏗️ Construction AI Scraper", layout="wide")
st.title("🏗️ Construction AI Tools Scraper")

# Inputs
query = st.text_input("🔍 Search query", value="construction AI tools")
mode = st.radio("📌 Mode", ["Resume", "Start fresh"])
run_button = st.button("🚀 Run Scraper")

OUTPUT_FILE = scraper.OUTPUT_FILE

# Helper to read CSV safely
def read_csv_safe(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame(columns=["Tool name","Description","Website","Source","Tags","Reviews","Launch date"])
    try:
        df = pd.read_csv(path, header=0, on_bad_lines="skip")
        # normalize headers if needed
        expected = ["tool_name","description","website","source","tags","reviews","launch_date"]
        if list(df.columns) != expected:
            df.columns = expected[:len(df.columns)]
        # pretty display
        df = df.rename(columns={
            "tool_name":"Tool name",
            "description":"Description",
            "website":"Website",
            "source":"Source",
            "tags":"Tags",
            "reviews":"Reviews",
            "launch_date":"Launch date"
        })
        return df
    except Exception as e:
        st.error(f"Error reading CSV: {e}")
        return pd.DataFrame(columns=["Tool name","Description","Website","Source","Tags","Reviews","Launch date"])

# Show current data
st.write("### 📊 Current scraped tools")
df = read_csv_safe(OUTPUT_FILE)
st.dataframe(df, use_container_width=True)

# Run
if run_button:
    with st.spinner("⏳ Running multi-engine scrape + GPT + Grok..."):
        total_saved, new_offset, items = scraper.run_scrape(query, mode)
    st.success(f"✅ Done! Saved {total_saved} new tools. Next offset: {new_offset}")

    # Reload table
    df = read_csv_safe(OUTPUT_FILE)
    st.write(f"### 📊 Updated scraped tools ({len(df)})")
    st.dataframe(df, use_container_width=True)

# Footnote
st.caption("Tip: Set API keys in your Streamlit app settings → Secrets. Supported: OPENAI_API_KEY, GROK_API_KEY, SERP_API_KEY, GOOGLE_API_KEY, GOOGLE_CSE_ID, BING_API_KEY.")