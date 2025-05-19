#!/usr/bin/env python3
"""
VivaTechnology Selected Sector Companies Scraper with Startup Flag and Per-Run Logging
====================================================================================
* partners ページをステップスクロールし、以下のカテゴリを含む企業の名前とリンクを抽出
  - Robotics
  - Healthcare & Wellness
  - Mobility & Smart Cities
* 各企業ブロック内に "startup" 表示があれば startup フラグを追加
* `sector_companies.csv` へ出力
* 実行ごとにユニークなログファイルを logs/ 以下に出力
"""
from __future__ import annotations

import csv
import time
import logging
import traceback
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------------------------------------------------------------
# Setup per-run logging
# ---------------------------------------------------------------------------
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
DEBUG_LOG = LOG_DIR / f"debug_{_timestamp}.log"
EXEC_LOG = LOG_DIR / f"execution_{_timestamp}.log"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Debug FileHandler
fh_debug = logging.FileHandler(DEBUG_LOG, encoding="utf-8")
fh_debug.setLevel(logging.DEBUG)
# Execution FileHandler
fh_info = logging.FileHandler(EXEC_LOG, encoding="utf-8")
fh_info.setLevel(logging.INFO)
# Console Handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
for handler in (fh_debug, fh_info, ch):
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://vivatechnology.com"
PAGE_URL = f"{BASE_URL}/partners"
HEADLESS = True
SCROLL_PAUSE = 0.8
# 抽出対象カテゴリ
TARGET_CATEGORIES = {"Robotics", "Healthcare & Wellness", "Mobility & Smart Cities"}

# ---------------------------------------------------------------------------
# Selenium helper
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
# Collect companies for target sectors
# ---------------------------------------------------------------------------
def collect_sector_companies() -> List[Dict[str, str]]:
    logger.info("Starting collection from %s", PAGE_URL)
    driver = _make_driver(HEADLESS)
    try:
        driver.get(PAGE_URL)
        logger.info("Page loaded: %s", PAGE_URL)

        # ステップスクロールで全体を表示
        total_height = driver.execute_script("return document.body.scrollHeight")
        viewport = driver.execute_script("return window.innerHeight")
        logger.debug("Page total height: %s, viewport height: %s", total_height, viewport)
        scroll_y = 0
        while scroll_y < total_height:
            driver.execute_script(f"window.scrollTo(0, {scroll_y});")
            logger.debug("Scrolled to Y: %s", scroll_y)
            time.sleep(SCROLL_PAUSE)
            scroll_y += viewport
            total_height = driver.execute_script("return document.body.scrollHeight")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        logger.info("Reached bottom of page after stepwise scrolling.")

        # 企業ブロック取得
        blocks = driver.find_elements(
            By.CSS_SELECTOR,
            "div.transition-all.duration-500.delay-100.h-full.opacity-100"
        )
        logger.info("Found %d total company blocks", len(blocks))

        results: List[Dict[str, str]] = []
        for idx, block in enumerate(blocks, start=1):
            try:
                # カテゴリ要素を探す
                cat_div = block.find_element(
                    By.CSS_SELECTOR,
                    "div.my-4.flex.flex-wrap.gap-2.items-center.justify-center"
                )
                categories = [span.text.strip() for span in cat_div.find_elements(
                    By.CSS_SELECTOR,
                    "span.flex-1.font-normal.text-clr-default-400.text-xs.px-2.truncate"
                )]
                # ターゲットカテゴリと一致するか
                if not TARGET_CATEGORIES.intersection(categories):
                    logger.debug("[%d] Skipped: %s", idx, categories)
                    continue

                # 企業名とリンクを抽出
                name_elem = block.find_element(By.CSS_SELECTOR, "h3 a")
                name = name_elem.text.strip()
                href = name_elem.get_attribute("href")
                partner_url = href if href.startswith("http") else BASE_URL + href

                # startup 判定
                try:
                    startup_div = block.find_element(
                        By.CSS_SELECTOR,
                        "div.w-full.flex"
                    )
                    startup_flag = "startup" if "startup" in startup_div.text.lower() else ""
                except Exception:
                    startup_flag = ""

                logger.info("[%d] Matched: %s -> %s (startup=%s, categories=%s)",
                            idx, name, partner_url, startup_flag, categories)
                results.append({
                    "name": name,
                    "partner_url": partner_url,
                    "startup": startup_flag,
                    "categories": ";".join(categories)
                })
            except Exception as e:
                logger.debug("[%d] Block error: %s", idx, e)

        logger.info("Collected %d companies", len(results))
        return results
    finally:
        driver.quit()
        logger.debug("WebDriver closed")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    try:
        companies = collect_sector_companies()
        out_path = Path("sector_companies.csv")
        logger.info("Writing results to %s", out_path)
        with out_path.open("w", newline="", encoding="utf-8") as fp:
            fieldnames = ["name", "partner_url", "startup", "categories"]
            writer = csv.DictWriter(fp, fieldnames=fieldnames)
            writer.writeheader()
            for row in companies:
                writer.writerow(row)
                logger.debug("Wrote row: %s", row)
        logger.info("Extraction complete: %s", out_path.resolve())
    except Exception:
        logger.error("Unexpected error:\n%s", traceback.format_exc())

if __name__ == "__main__":
    main()
