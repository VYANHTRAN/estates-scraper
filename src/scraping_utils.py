import os
import json
import time
import sys
import threading
import requests
import itertools
from bs4 import BeautifulSoup
from tqdm import tqdm
from fake_useragent import UserAgent

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from src.config import *
from src.db_utils import DatabaseManager


class Scraper:
    def __init__(self):
        self.ua = UserAgent()
        self.db = DatabaseManager()
        self.stop_requested = threading.Event()

        self.browser = None
        self.context = None
        self.page = None

        self.all_scraped_urls = set()
        self.fieldnames = [
            "listing_title", "property_id", "total_price",
            "unit_price", "property_url", "image_url",
            "city", "district", "alley_width",
            "features", "property_description"
        ]

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        self._init_browser()

    # ------------------------------------------------------------------
    # Browser setup
    # ------------------------------------------------------------------
    def _init_browser(self):
        self.log("Initializing Playwright browser...", "INFO")
        try:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-extensions",
                ],
            )
            self.context = self.browser.new_context(
                user_agent=self.ua.random,
                viewport={"width": 1920, "height": 1080},
            )
            self.page = self.context.new_page()
        except Exception as e:
            self.log(f"Failed to initialize Playwright: {e}", "CRITICAL")
            sys.exit(1)

    def shutdown(self):
        self.log("Shutting down...", "INFO")
        self.stop_requested.set()
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if hasattr(self, "playwright"):
            self.playwright.stop()
        self.db.close()

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    def log(self, message, level="INFO"):
        levels = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
        if levels.index(level.upper()) >= levels.index(LOG_LEVEL.upper()):
            print(f"[{level}] {message}")

    # ------------------------------------------------------------------
    # Menu scraping 
    # ------------------------------------------------------------------
    def get_listing_urls(self, html):
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select('a[data-role="property-card"]')
        urls = [
            BASE_URL + card.get("href")
            if card.get("href") and not card.get("href").startswith("http")
            else card.get("href")
            for card in cards if card.get("href")
        ]
        return urls

    def scrape_menu_pages(self):
        page_num = START_PAGE

        if END_PAGE > 0:
            page_range = range(START_PAGE, END_PAGE + 1)
        else:
            page_range = itertools.count(START_PAGE)

        for _ in tqdm(page_range, desc="Scraping menu pages"):
            if self.stop_requested.is_set():
                break

            url = f"{START_URL}page={page_num}"
            consecutive_http_errors = 0
            consecutive_empty_pages = 0

            for retry in range(MAX_RETRIES):
                try:
                    headers = {"User-Agent": self.ua.random}
                    response = requests.get(url, headers=headers, timeout=10)

                    if 400 <= response.status_code < 600:
                        consecutive_http_errors += 1
                        if consecutive_http_errors >= 3:
                            self.log("Too many HTTP errors. Stopping.", "CRITICAL")
                            self.stop_requested.set()
                            break
                        raise Exception(f"Status {response.status_code}")

                    if not response.text:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= 3:
                            self.log("Too many empty pages. Stopping.", "CRITICAL")
                            self.stop_requested.set()
                            break
                        raise Exception("Empty response")

                    consecutive_http_errors = 0
                    consecutive_empty_pages = 0

                    links = self.get_listing_urls(response.text)
                    self.all_scraped_urls.update(links)

                    if END_PAGE <= 0 and not links:
                        return self.all_scraped_urls

                    break

                except Exception as e:
                    self.log(
                        f"Retry {retry + 1}/{MAX_RETRIES} page {page_num}: {e}",
                        "WARN",
                    )
                    time.sleep(RETRY_DELAY)

            page_num += 1

        return self.all_scraped_urls

    def save_urls(self, urls_to_save):
        if not urls_to_save:
            return
        with open(URLS_OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(list(urls_to_save), f, ensure_ascii=False, indent=2)
        self.log(f"Saved {len(urls_to_save)} URLs", "INFO")

    # ------------------------------------------------------------------
    # Details scraping 
    # ------------------------------------------------------------------
    def extract_listing_details(self, url):
        if self.stop_requested.is_set():
            return None

        try:
            self.page.goto(url, timeout=30000)
            self.page.wait_for_selector("body", timeout=10000)

            def safe_text(selector):
                try:
                    el = self.page.wait_for_selector(selector, timeout=5000)
                    return el.inner_text().strip()
                except Exception:
                    return None

            data = {
                "listing_title": safe_text("#detail_title"),
                "property_id": safe_text(
                    "#container-property div:nth-child(5) div.flex.cursor-pointer p"
                ),
                "total_price": safe_text("#total-price"),
                "unit_price": safe_text("#unit-price"),
                "property_url": url,
                "alley_width": safe_text(
                    '#overview_content div[data-impression-index="1"]'
                ),
                "image_url": None,
                "city": None,
                "district": None,
                "features": [],
                "property_description": "",
            }

            # Image
            img = self.page.query_selector(
                'link[rel="preload"][as="image"]'
            )
            if img:
                srcset = img.get_attribute("imagesrcset")
                if srcset:
                    data["image_url"] = srcset.split(",")[0].split(" ")[0]

            # Breadcrumbs (JSON-LD)
            scripts = self.page.query_selector_all(
                'script[type="application/ld+json"]'
            )
            for s in scripts:
                try:
                    jd = json.loads(s.inner_text())
                    if isinstance(jd, dict) and jd.get("@type") == "BreadcrumbList":
                        for item in jd.get("itemListElement", []):
                            if item.get("position") == 2:
                                data["city"] = item.get("name")
                            elif item.get("position") == 3:
                                data["district"] = item.get("name")
                        break
                except Exception:
                    continue

            # Features
            feature_items = self.page.query_selector_all("#key-feature-item")
            for ele in feature_items:
                try:
                    title = ele.query_selector("#item_title").inner_text().strip()
                    text = ele.query_selector("#key-feature-text").inner_text().strip()
                    if title and text:
                        data["features"].append(f"{title}: {text}")
                except Exception:
                    continue

            # Description
            try:
                seo_title = self.page.inner_text(
                    'span[data-testid="seo-title-meta"]'
                )
                seo_description = self.page.inner_text(
                    'span[data-testid="seo-description-meta"]'
                )

                li_elements = self.page.query_selector_all(
                    'ul[aria-label="description-heading"] li'
                )
                content = [li.inner_text().strip() for li in li_elements]

                container = self.page.wait_for_selector(
                    '//span[@aria-label="main-street-name-heading"]/ancestor::div[contains(@class,"text-om-t16")]',
                    timeout=5000,
                )
                raw_text = container.text_content()

                parts = [
                    seo_title,
                    seo_description,
                    " ".join(content),
                    raw_text,
                ]
                data["property_description"] = " ".join(
                    p.strip() for p in parts if p and p.strip()
                )
            except Exception:
                pass

            self.log(f"Extracted details for {url}", "DEBUG")
            return data

        except PlaywrightTimeoutError as e:
            self.log(f"Timeout for {url}: {e}", "WARN")
            return None

    def scrape_with_retries(self, url):
        for _ in range(MAX_RETRIES):
            if self.stop_requested.is_set():
                break
            result = self.extract_listing_details(url)
            if result:
                return result
            time.sleep(RETRY_DELAY)
        return None

    def process_listings_from_json(self, json_path):
        if not os.path.exists(json_path):
            self.log(f"JSON not found: {json_path}", "ERROR")
            return

        with open(json_path, "r", encoding="utf-8") as f:
            urls = json.load(f)

        for url in tqdm(urls, desc="Scraping details"):
            if self.stop_requested.is_set():
                break
            result = self.scrape_with_retries(url)
            if result:
                self.db.save_listing(result, self.fieldnames)
