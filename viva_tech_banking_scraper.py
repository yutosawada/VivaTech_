#!/usr/bin/env python3
"""
VivaTechnology multi‑sector scraper
===================================
抽出対象セクター:
  - Consumer Goods / Retail / E‑commerce
  - Health
  - Industry
  - Information Technologies
  - Luxury / Fashion / Beauty
  - Luxury / Fashion / Beauty | Marketing / Advertising / Communication
  - Mobility / Transportation
  - Mobility / Transportation | Smart City / Building

* 各セクター一覧ページを順番にスクロールして出展企業 URL を収集
* 企業ページから以下を抽出
    - name, booth, homepage, categories, overview, **startup**, partner_url
* `viva_partners.csv` へ出力しつつ `Company -> Homepage` をリアルタイム表示
"""
from __future__ import annotations

import csv
import re
import time
import warnings
from pathlib import Path
from typing import Dict, List, Set

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://vivatechnology.com"
LIST_URLS = [
    "https://vivatechnology.com/partners?sectors=consumer%2520goods%252Fretail%252Fe-commerce",
    "https://vivatechnology.com/partners?sectors=health",
    "https://vivatechnology.com/partners?sectors=industry",
    "https://vivatechnology.com/partners?sectors=information%2520technologies",
    "https://vivatechnology.com/partners?sectors=luxury%252Ffashion%252Fbeauty",
    "https://vivatechnology.com/partners?sectors=marketing%252Fadvertising%252Fcommunication",
    "https://vivatechnology.com/partners?sectors=mobility%252Ftransportation",
    "https://vivatechnology.com/partners?sectors=smart%2520city%252Fbuilding",

]

HEADLESS = True
SCROLL_STEP = 800
SCROLL_PAUSE = 0.8
MAX_SCROLL_LOOPS = 600
STABLE_THRESHOLD = 4
REQUEST_PAUSE = 0.4
MAX_RETRIES = 3

# domains to exclude
SOCIAL_RE = re.compile(
    r"linkedin|youtube|instagram|facebook|twitter|cookiebot|cookieyes|airtable|intercom", re.I
)

# ---------------------------------------------------------------------------
# Selenium helpers
# ---------------------------------------------------------------------------

def _make_driver(headless: bool = True) -> webdriver.Chrome:
    """Spin up a new Chrome driver instance."""
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--lang=en")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

# ---------------------------------------------------------------------------
# Collect partner URLs from all sector pages
# ---------------------------------------------------------------------------

def _current_partner_links(driver: webdriver.Chrome) -> Set[str]:
    """Return the set of company profile URLs currently in the viewport."""
    return {
        a.get_attribute("href").split("?")[0]
        for a in driver.find_elements(By.CSS_SELECTOR, 'a[href^="/partners/"]')
        if a.get_attribute("href")
    }

def collect_all_partner_urls() -> List[str]:
    """Scroll each sector list page to collect all partner profile links."""
    all_links: Set[str] = set()
    for page_url in LIST_URLS:
        print(f"Scanning sector page → {page_url}")
        driver = _make_driver(HEADLESS)
        driver.get(page_url)

        last_count = -1
        stable_rounds = 0
        for _ in range(MAX_SCROLL_LOOPS):
            driver.execute_script(f"window.scrollBy(0, {SCROLL_STEP});")
            time.sleep(SCROLL_PAUSE)

            links = _current_partner_links(driver)
            if len(links) == last_count:
                stable_rounds += 1
                if stable_rounds >= STABLE_THRESHOLD:
                    break
            else:
                stable_rounds = 0
                last_count = len(links)

            at_bottom = driver.execute_script(
                "return (window.innerHeight + window.scrollY) >= document.body.scrollHeight - 2"
            )
            if at_bottom and stable_rounds >= 1:
                break

        print(f"  found {len(links)} links on this page")
        all_links.update(links)
        driver.quit()
    print(f"Total unique partner profiles collected: {len(all_links)}")
    return sorted(all_links)

# ---------------------------------------------------------------------------
# Fetch & parse partner page
# ---------------------------------------------------------------------------

def _fetch_html(url: str) -> str:
    """Download a partner profile page and return its HTML."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            d = _make_driver(HEADLESS)
            d.get(url)
            WebDriverWait(d, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.ml-1.uppercase"))
            )
            html = d.page_source
            d.quit()
            return html
        except WebDriverException as exc:
            warnings.warn(f"{url} → {exc} – retry {attempt}/{MAX_RETRIES}")
            time.sleep(2 * attempt)
    raise RuntimeError(f"Failed to fetch {url}")

# ---------------------------------------------------------------------------
# Field extractors
# ---------------------------------------------------------------------------

def _first_homepage(soup: BeautifulSoup) -> str:
    """Extract the first non-social external link as the company's homepage."""
    for a in soup.select("a[href]"):
        if a.find("span", class_="label symbols") and "language" in a.get_text(" ", strip=True).lower():
            href = a["href"].strip()
            if href.startswith("http") and not SOCIAL_RE.search(href):
                return href
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if (
            href.startswith("http")
            and "vivatechnology.com" not in href
            and not SOCIAL_RE.search(href)
            and "privacy" not in href.lower()
            and "cookie" not in href.lower()
        ):
            return href
    return ""


def _extract_booth(soup: BeautifulSoup) -> str:
    for sel in [
        "div.text-xs span.ml-1.uppercase",
        "span.symbols + span.ml-1.uppercase",
        "span.ml-1.uppercase",
    ]:
        tag = soup.select_one(sel)
        if tag and tag.get_text(strip=True):
            return tag.get_text(strip=True).strip('"“”')
    return ""


def _extract_categories(soup: BeautifulSoup) -> str:
    cats = [span.get_text(strip=True) for span in soup.select(
        "span.flex-1.font-normal.text-clr-default-400.text-xs.px-2.truncate")]
    seen: Set[str] = set()
    return ", ".join([c for c in cats if not (c in seen or seen.add(c))])


def _extract_overview(soup: BeautifulSoup) -> str:
    parts = [div.get_text(" ", strip=True) for div in soup.select("div.my-4.text-xs.leading-relaxed")]
    return "\n".join(parts)


def _is_startup(soup: BeautifulSoup) -> str:
    """Return 'startup' if the profile is labelled as such, else empty string."""
    # Precise tag (keeps false‑positives low)
    tag = soup.select_one(
        "div.max-w-fit.min-w-min.justify-between.box-border.whitespace-nowrap"
        ".px-2.text-medium.rounded-full.bg-clr-quaternary-50.text-clr-quaternary-700"
    )
    if tag and "startup" in tag.get_text(strip=True).lower():
        return "startup"

    # Fallback: any 'startup' badge elsewhere
#    if soup.find(string=re.compile(r"\bstartup\b", re.I)):
#        return "startup"
    return ""

# ---------------------------------------------------------------------------
# Parse one partner
# ---------------------------------------------------------------------------

def parse_partner(url: str) -> Dict[str, str]:
    soup = BeautifulSoup(_fetch_html(url), "html.parser")
    name = soup.find("h1").get_text(strip=True) if soup.find("h1") else url.rsplit("/", 1)[-1]
    return {
        "name": name,
        "booth": _extract_booth(soup),
        "homepage": _first_homepage(soup),
        "categories": _extract_categories(soup),
        "overview": _extract_overview(soup),
        "startup": _is_startup(soup),
        "partner_url": url,
    }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    partner_urls = collect_all_partner_urls()
    rows: List[Dict[str, str]] = []
    for url in tqdm(partner_urls, desc="Scraping", unit="profile"):
        try:
            row = parse_partner(url)
            rows.append(row)
            print(f"{row['name']} -> {row['homepage']}")
            time.sleep(REQUEST_PAUSE)
        except Exception as exc:
            warnings.warn(f"{url} failed → {exc}")

    out = Path("viva_partners.csv")
    with out.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
                "name",
                "booth",
                "homepage",
                "categories",
                "overview",
                "startup",
                "partner_url",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Scraping complete → {out.resolve()}")


if __name__ == "__main__":
    main()
