import re
import time
from collections import deque
from urllib.parse import urljoin, urlparse

import gspread
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

# --- PAGE CONFIG ---
st.set_page_config(page_title="Smart Web Scraper", page_icon="🕷️", layout="centered")

# --- CSS FOR "DONKEY PROOF" UI ---
st.markdown(
    """
    <style>
    .stButton>button {
        width: 100%;
        background-color: #FF4B4B;
        color: white;
        height: 3em;
        font-weight: bold;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        color: #155724;
        border: 1px solid #c3e6cb;
    }
    </style>
""",
    unsafe_allow_html=True,
)

# --- SETTINGS (Can be moved to sidebar) ---
DEFAULT_SHEET_URL = "https://docs.google.com/spreadsheets/d/13_qBxI3JJj4ekoxnzRuTXjpxLkGLpL4UQdnK_RDmj8E/edit?gid=120173082#gid=120173082"
CREDENTIALS_FILE = "credentials.json"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# --- HELPER FUNCTIONS ---


def setup_google_sheets(sheet_url, creds_file):
    """Authenticates with Google Sheets using Secrets (Prod) or File (Local)."""
    try:
        # Check if running in Streamlit Cloud with Secrets
        if "gcp_service_account" in st.secrets:
            service_account_info = st.secrets["gcp_service_account"]
            # Convert to dict in case it's a generic Streamlit secrets object
            # gspread handles the dict format directly
            gc = gspread.service_account_from_dict(service_account_info)
        else:
            # Fallback to local file
            gc = gspread.service_account(filename=creds_file)

        sh = gc.open_by_url(sheet_url)
        # Select the first worksheet
        return sh.get_worksheet(0), None
    except Exception as e:
        return None, str(e)


def clean_text(text):
    return re.sub(r"\s+", " ", text).strip()


def get_structured_data(soup, keywords):
    results = []
    for kw in keywords:
        # Search in text nodes
        matches = soup.find_all(string=re.compile(re.escape(kw), re.IGNORECASE))
        seen_blocks = set()

        for match in matches:
            element = match.parent

            # 1. Table Row
            tr = element.find_parent("tr")
            if tr:
                cells = [clean_text(td.get_text()) for td in tr.find_all(["td", "th"])]
                block_text = " | ".join(cells)
                context_type = "Table Row"

            # 2. Section Header
            elif element.name in [
                "dt",
                "b",
                "strong",
                "h1",
                "h2",
                "h3",
                "h4",
                "h5",
                "h6",
            ]:
                header = clean_text(element.get_text())
                next_node = element.find_next_sibling()
                value = clean_text(next_node.get_text()) if next_node else ""
                block_text = (
                    f"{header}: {value}"
                    if value
                    else clean_text(element.parent.get_text())
                )
                context_type = "Section Header"

            # 3. List Item
            elif element.find_parent("li"):
                li = element.find_parent("li")
                block_text = clean_text(li.get_text(separator=" "))
                context_type = "List Item"

            # 4. Paragraph (Fallback)
            else:
                container = element
                while container.parent and container.name not in [
                    "p",
                    "div",
                    "article",
                ]:
                    container = container.parent
                    if container.name == "body":
                        break
                block_text = clean_text(container.get_text(separator=" "))
                # Truncate
                if len(block_text) > 300:
                    start_idx = block_text.lower().find(kw.lower())
                    start = max(0, start_idx - 50)
                    end = min(len(block_text), start_idx + 150)
                    block_text = "..." + block_text[start:end] + "..."
                context_type = "Text Block"

            if len(block_text) < 3 or "copyright" in block_text.lower():
                continue

            if block_text not in seen_blocks:
                results.append(
                    {"keyword": kw, "context": block_text, "type": context_type}
                )
                seen_blocks.add(block_text)
    return results


def find_relevant_links(base_url, soup, keywords):
    relevant_links = set()
    domain = urlparse(base_url).netloc
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(" ", strip=True).lower()
        full_url = urljoin(base_url, href)
        if urlparse(full_url).netloc != domain:
            continue
        for kw in keywords:
            if kw.lower() in text or kw.lower() in href.lower():
                relevant_links.add(full_url)
                break
    return relevant_links


def scrape_logic(base_url, keywords_list, status_container):
    """
    Main logic wrapped for UI consumption.
    status_container: streamlit empty placeholder for live updates
    """
    all_data = []
    visited_urls = set()

    # Phase 1
    status_container.info(f"Phase 1: Analyzing Home Page: {base_url}")
    try:
        response = requests.get(base_url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(response.text, "html.parser")

        # Cleanup
        for tag in soup(["script", "style", "nav", "footer", "noscript"]):
            tag.decompose()

        home_data = get_structured_data(soup, keywords_list)
        child_links = find_relevant_links(base_url, soup, keywords_list)

        for item in home_data:
            item["url"] = base_url
            all_data.append(item)
        visited_urls.add(base_url)

    except Exception as e:
        st.error(f"Error accessing home page: {e}")
        return []

    # Phase 2
    if child_links:
        status_container.info(
            f"Phase 2: Found {len(child_links)} relevant pages. Crawling..."
        )
        progress_bar = st.progress(0)
        total_links = len(child_links)

        for i, link in enumerate(child_links):
            if link in visited_urls:
                continue

            # Update status
            status_container.markdown(f"**Scanning:** `{link}`")
            progress_bar.progress((i + 1) / total_links)

            time.sleep(1.0)  # Polite delay

            try:
                resp = requests.get(link, headers=HEADERS, timeout=10)
                sub_soup = BeautifulSoup(resp.text, "html.parser")
                for tag in sub_soup(["script", "style", "nav", "footer"]):
                    tag.decompose()

                page_data = get_structured_data(sub_soup, keywords_list)
                for item in page_data:
                    item["url"] = link
                    all_data.append(item)
            except:
                pass  # Skip errors on sub-pages

            visited_urls.add(link)

        progress_bar.empty()
    else:
        status_container.warning("No relevant sub-links found. Only checked Home Page.")

    return all_data


# --- UI LAYOUT ---

st.title("🕷️ Smart Web Scraper")
st.markdown(
    "Enter a URL and keywords to extract structured data directly to Google Sheets."
)

with st.expander("⚙️ Advanced Settings (Credentials & Sheet URL)"):
    # Intelligent check for Cloud Secrets
    if "gcp_service_account" in st.secrets:
        st.success("✅ Cloud Credentials Loaded (Secrets Mode)")
        creds_file_input = "secrets"  # Placeholder
    else:
        # Local Mode
        creds_file_input = st.text_input("Credentials Filename", value=CREDENTIALS_FILE)

    sheet_url_input = st.text_input("Google Sheet URL", value=DEFAULT_SHEET_URL)

col1, col2 = st.columns([2, 1])

with col1:
    url_input = st.text_input("Target URL", placeholder="https://www.hdfcbank.com")

with col2:
    keywords_input = st.text_input(
        "Keywords (comma separated)", placeholder="Millennia, Fees, Interest"
    )

if st.button("Start Scraping"):
    # 1. Validation
    if not url_input or not keywords_input:
        st.error("⚠️ Please enter both a URL and Keywords.")
    else:
        keywords_list = [k.strip() for k in keywords_input.split(",") if k.strip()]

        # 2. Check Credentials First
        worksheet, err = setup_google_sheets(sheet_url_input, creds_file_input)

        if err:
            st.error(f"❌ Authentication Error: {err}")
            if "gcp_service_account" not in st.secrets:
                st.info(
                    "Make sure 'credentials.json' is in the folder and shared with the sheet."
                )
            else:
                st.info("Check your Streamlit Secrets configuration.")
        else:
            # 3. Run Scraper
            status_area = st.empty()
            with st.spinner("Scraping in progress..."):
                results = scrape_logic(url_input, keywords_list, status_area)

            # 4. Handle Results
            if results:
                status_area.success(
                    f"✅ Scanning Complete. Found {len(results)} data points."
                )

                # Preview Data
                df = pd.DataFrame(results)
                st.dataframe(df[["keyword", "context", "url"]].head(10))

                # Export to Sheet
                with st.spinner("Exporting to Google Sheets..."):
                    try:
                        # Header Check
                        if not worksheet.get("A1"):
                            worksheet.append_row(
                                [
                                    "Source URL",
                                    "Keyword Matched",
                                    "Extracted Context",
                                    "Type",
                                    "Timestamp",
                                ]
                            )

                        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                        rows = []
                        for r in results:
                            rows.append(
                                [
                                    r["url"],
                                    r["keyword"],
                                    r["context"],
                                    r["type"],
                                    timestamp,
                                ]
                            )

                        worksheet.append_rows(rows)
                        st.markdown(
                            f'<div class="success-box">🎉 Success! Data exported to <a href="{sheet_url_input}" target="_blank">Google Sheets</a></div>',
                            unsafe_allow_html=True,
                        )
                        st.balloons()

                    except Exception as e:
                        st.error(f"Failed to write to Sheet: {e}")
            else:
                status_area.warning("No data found matching those keywords.")
