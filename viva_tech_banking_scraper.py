#!/usr/bin/env python3
"""
VivaTechnology Selected Sector Companies Scraper with Detailed Company Page Extraction
====================================================================================
* partners ページをステップスクロールし、以下のカテゴリを含む企業を抽出
  - 複数カテゴリ対応
* 各企業ページへアクセスし、以下の情報を取得
  - booth
  - homepage（最初の非ソーシャル外部リンク）
  - categories（詳細）
  - overview
  - startup フラグ
* 最終的に各カテゴリーごとの抽出社数をログ出力
* `sector_companies.csv` へ出力
* 実行ごとにユニークなログを logs/ 以下に出力
"""
from __future__ import annotations

import csv
import time
import logging
import traceback
import re
import warnings
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://vivatechnology.com"
PAGE_URL = f"{BASE_URL}/partners"
HEADLESS = True
SCROLL_PAUSE = 0.8
# 抽出対象カテゴリ（複数対応可）
TARGET_CATEGORIES: Set[str] = {
    "Artificial Intelligence",
    "Connectivity, Cloud & Infrastructure",
    "Cybersecurity",
    "Deep Tech & Quantum Computing",
    "Edtech",
    "Energy",
    "Food & Agriculture",
    "HR & Future of Work",
    "Healthcare & Wellness",
    "Industry & Supply Chain",
    "Luxury, Fashion & Cosmetics",
    "Marketing & Advertising",
    "Mobility & Smart Cities",
    "Retail & E-commerce",
    "Space, Aeronautics & Defense",
}
MAX_RETRIES = 3

# ---------------------------------------------------------------------------
# Logging setup per run
# ---------------------------------------------------------------------------
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
DEBUG_LOG = LOG_DIR / f"debug_{_timestamp}.log"
EXEC_LOG = LOG_DIR / f"execution_{_timestamp}.log"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh_debug = logging.FileHandler(DEBUG_LOG, encoding="utf-8")
fh_debug.setLevel(logging.DEBUG)
fh_info = logging.FileHandler(EXEC_LOG, encoding="utf-8")
fh_info.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
for h in (fh_debug, fh_info, ch):
    h.setFormatter(formatter)
    logger.addHandler(h)

# ---------------------------------------------------------------------------
# Selenium driver helper
# ---------------------------------------------------------------------------
def _make_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--lang=en")
    service = Service(ChromeDriverManager().install())
    logger.debug("Initializing Chrome WebDriver (headless=%s)", headless)
    return webdriver.Chrome(service=service, options=opts)

# ---------------------------------------------------------------------------
# Scroll full page stepwise
# ---------------------------------------------------------------------------
def _scroll_full_page(driver: webdriver.Chrome) -> None:
    total = driver.execute_script("return document.body.scrollHeight")
    view = driver.execute_script("return window.innerHeight")
    pos = 0
    while pos < total:
        driver.execute_script(f"window.scrollTo(0, {pos});")
        time.sleep(SCROLL_PAUSE)
        pos += view
        total = driver.execute_script("return document.body.scrollHeight")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    logger.info("Finished stepwise scroll")

# ---------------------------------------------------------------------------
# Basic sector page extraction
# ---------------------------------------------------------------------------
def collect_sector_companies() -> List[Dict[str, str]]:
    driver = _make_driver(HEADLESS)
    companies: List[Dict[str, str]] = []
    try:
        driver.get(PAGE_URL)
        logger.info("Loaded page %s", PAGE_URL)
        _scroll_full_page(driver)
        blocks = driver.find_elements(By.CSS_SELECTOR, "div.transition-all.duration-500.delay-100.h-full.opacity-100")
        logger.info("Found %d company blocks", len(blocks))
        for idx, b in enumerate(blocks, start=1):
            try:
                cat_div = b.find_element(By.CSS_SELECTOR, "div.my-4.flex.flex-wrap.gap-2.items-center.justify-center")
                cats = [s.text.strip() for s in cat_div.find_elements(By.CSS_SELECTOR, "span.flex-1.font-normal.text-clr-default-400.text-xs.px-2.truncate")]
                if not TARGET_CATEGORIES.intersection(cats):
                    continue
                name_el = b.find_element(By.CSS_SELECTOR, "h3 a")
                name = name_el.text.strip()
                href = name_el.get_attribute("href")
                url = href if href.startswith("http") else BASE_URL + href
                try:
                    st = b.find_element(By.CSS_SELECTOR, "div.w-full.flex").text
                    startup_flag = "startup" if "startup" in st.lower() else ""
                except Exception:
                    startup_flag = ""
                companies.append({"name": name, "url": url, "startup": startup_flag})
            except Exception as e:
                logger.debug("skip block %d: %s", idx, e)
        logger.info("Collected %d companies for detail extraction", len(companies))
        return companies
    finally:
        driver.quit()

# ---------------------------------------------------------------------------
# Fetch HTML with retries
# ---------------------------------------------------------------------------
def _fetch_html(url: str) -> str:
    for i in range(1, MAX_RETRIES+1):
        try:
            d = _make_driver(HEADLESS)
            d.get(url)
            time.sleep(1)
            html = d.page_source
            d.quit()
            return html
        except Exception as ex:
            warnings.warn(f"Fetch attempt {i} failed: {ex}")
            time.sleep(2*i)
    raise RuntimeError(f"Failed to fetch {url}")

# ---------------------------------------------------------------------------
# Page parsing utilities
# ---------------------------------------------------------------------------
def _first_homepage(soup: BeautifulSoup) -> str:
    for a in soup.select("a[href]"):
        if a.find("span", class_="label symbols") and "language" in a.get_text(" ", strip=True).lower():
            href = a["href"].strip()
            if href.startswith("http") and not SOCIAL_RE.search(href):
                return href
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("http") and "vivatechnology.com" not in href and not SOCIAL_RE.search(href) and "privacy" not in href.lower() and "cookie" not in href.lower():
            return href
    return ""


def _extract_booth(soup: BeautifulSoup) -> str:
    for sel in ["div.text-xs span.ml-1.uppercase","span.symbols + span.ml-1.uppercase","span.ml-1.uppercase"]:
        tag = soup.select_one(sel)
        if tag:
            txt = tag.get_text(strip=True).strip('"“”')
            if txt:
                return txt
    return ""


def _extract_categories(soup: BeautifulSoup) -> str:
    cats = [span.get_text(strip=True) for span in soup.select("span.flex-1.font-normal.text-clr-default-400.text-xs.px-2.truncate")]
    seen: Set[str] = set()
    return ", ".join([c for c in cats if not (c in seen or seen.add(c))])


def _extract_overview(soup: BeautifulSoup) -> str:
    parts = [div.get_text(" ", strip=True) for div in soup.select("div.my-4.text-xs.leading-relaxed")]\

    return "\n".join(parts)

# ---------------------------------------------------------------------------
# Detailed extraction
# ---------------------------------------------------------------------------
def enrich_companies(companies: List[Dict[str, str]]) -> List[Dict[str, str]]:
    enriched: List[Dict[str, str]] = []
    for idx, comp in enumerate(companies, start=1):
        try:
            html = _fetch_html(comp["url"])
            soup = BeautifulSoup(html, "html.parser")
            booth = _extract_booth(soup)
            homepage = _first_homepage(soup)
            cats = _extract_categories(soup)
            overview = _extract_overview(soup)
            enriched.append({**comp, "booth": booth, "homepage": homepage, "categories_detail": cats, "overview": overview})
        except Exception as e:
            logger.debug("enrich error for %s: %s", comp["url"], e)
    return enriched

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    try:
        base = collect_sector_companies()
        full = enrich_companies(base)
        # カテゴリごとの件数カウント
        counts: Dict[str, int] = {cat: 0 for cat in TARGET_CATEGORIES}
        for row in full:
            for cat in TARGET_CATEGORIES:
                if cat in row.get("categories_detail", ""):
                    counts[cat] += 1
        logger.info("Category counts:")
        for cat, cnt in counts.items():
            logger.info("  %s: %d companies", cat, cnt)
        # CSV 出力
        out = Path("sector_companies.csv")
        fields = ["name","url","startup","booth","homepage","categories_detail","overview"]
        with out.open("w", newline="", encoding="utf-8") as fp:
            writer = csv.DictWriter(fp, fieldnames=fields)
            writer.writeheader()
            for row in full:
                writer.writerow(row)
        logger.info("Saved %d rows to %s", len(full), out.resolve())
    except Exception:
        logger.error("Error in main:\n%s", traceback.format_exc())

if __name__ == "__main__":
    main()
