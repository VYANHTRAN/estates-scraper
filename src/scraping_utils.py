import os
import json
import time
import sys
import threading
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from fake_useragent import UserAgent

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

from src.config import *
from src.db_utils import DatabaseManager


class Scraper:
    def __init__(self):
        self.ua = UserAgent()
        self.db = DatabaseManager()
        self.driver = None
        self.stop_requested = threading.Event()
        
        self.all_scraped_urls = set()
        self.fieldnames = [
            "listing_title", "property_id", "total_price",
            "unit_price", "property_url", "image_url",
            "city", "district", "alley_width",
            "features", "property_description"
        ]
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        self._init_driver()

    def _init_driver(self):
        """Initializes the single WebDriver instance."""
        self.log("Initializing WebDriver...", "INFO")
        options = Options()
        options.add_argument(f"user-agent={self.ua.random}")
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,1080")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        try:
            self.driver = webdriver.Chrome(options=options)
        except Exception as e:
            self.log(f"Failed to initialize WebDriver: {e}", "CRITICAL")
            sys.exit(1)

    def log(self, message, level="INFO"):
        levels = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
        if levels.index(level.upper()) >= levels.index(LOG_LEVEL.upper()):
            print(f"[{level}] {message}")


    # -------------------- Menu Scraping --------------------
    def get_listing_urls(self, html):
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select('a[data-role="property-card"]')
        urls = [
            BASE_URL + card.get("href") if card.get("href") and not card.get("href").startswith("http") else card.get("href")
            for card in cards if card.get("href")
        ]
        return urls

    def scrape_menu_pages(self):
        page_num = START_PAGE

        if END_PAGE > 0:
            page_range = range(START_PAGE, END_PAGE + 1) # set fixed range 
        else:
            page_range = iter(int, 1)  # infinite iterator to scrape until there is no listing 

        for _ in tqdm(page_range, desc="Scraping menu pages"):
            if self.stop_requested.is_set():
                self.log("Stop requested. Exiting URL scraping.", "INFO")
                break

            url = f"{START_URL}page={page_num}"
            consecutive_http_errors = 0
            consecutive_empty_pages = 0

            for retry in range(MAX_RETRIES):
                if self.stop_requested.is_set():
                    break

                try:
                    headers = {"User-Agent": self.ua.random} 
                    response = requests.get(url, headers=headers, timeout=10)

                    # HTTP error handling 
                    if 400 <= response.status_code < 600:
                        consecutive_http_errors += 1
                        if consecutive_http_errors >= 3:
                            self.log(
                                f"Critical: {consecutive_http_errors} HTTP errors at {url}. Stopping.",
                                "CRITICAL",
                            )
                            self.stop_requested.set()
                            break
                        raise Exception(f"Status code {response.status_code}")

                    # Empty response
                    if not response.text:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= 3:
                            self.log(
                                f"Critical: {consecutive_empty_pages} empty pages at {url}. Stopping.",
                                "CRITICAL",
                            )
                            self.stop_requested.set()
                            break
                        raise Exception("Empty page content")

                    consecutive_http_errors = 0
                    consecutive_empty_pages = 0

                    links = self.get_listing_urls(response.text)
                    self.log(
                        f"Extracted {len(links)} links from page {page_num}",
                        "DEBUG",
                    )
                    
                    if END_PAGE <= 0 and not links:
                        self.log(
                            f"No listings found on page {page_num}. Reached end of pages.",
                            "INFO",
                        )
                        return self.all_scraped_urls

                    self.all_scraped_urls.update(links)
                    break

                except requests.exceptions.RequestException as e:
                    self.log(
                        f"Retry {retry + 1}/{MAX_RETRIES} for page {page_num}: {e}",
                        "WARN",
                    )
                    time.sleep(RETRY_DELAY)

                except Exception as e:
                    self.log(
                        f"Retry {retry + 1}/{MAX_RETRIES} for page {page_num}: {e}",
                        "WARN",
                    )
                    time.sleep(RETRY_DELAY)

            else:
                self.log(
                    f"Failed to fetch page {page_num} after {MAX_RETRIES} retries.",
                    "ERROR",
                )

            page_num += 1

        return self.all_scraped_urls
            
    def save_urls(self, urls_to_save):
        if not urls_to_save:
            self.log("No URLs to save.", "INFO")
            return
        with open(URLS_OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(list(urls_to_save), f, ensure_ascii=False, indent=2)
        self.log(f"Saved {len(urls_to_save)} URLs to {URLS_OUTPUT_PATH}", "INFO")


    # -------------------- Details Scraping --------------------
    def extract_listing_details(self, url):
        if self.stop_requested.is_set(): return None
        
        try:
            self.driver.get(url)
            wait = WebDriverWait(self.driver, 10)
            wait.until(EC.presence_of_element_located((By.XPATH, "/html/body")))

            def safe_text(by, selector):
                try:
                    return wait.until(EC.presence_of_element_located((by, selector))).text.strip()
                except: return None

            data = {
                "listing_title": safe_text(By.XPATH, '//*[@id="detail_title"]'),
                "property_id": safe_text(By.CSS_SELECTOR, '#container-property div:nth-child(5) div.flex.cursor-pointer p'),
                "total_price": safe_text(By.XPATH, '//*[@id="total-price"]'),
                "unit_price": safe_text(By.XPATH, '//*[@id="unit-price"]'),
                "property_url": url,
                "alley_width": safe_text(By.XPATH, '//*[@id="overview_content"]//div[@data-impression-index="1"]'),
                "image_url": None,
                "city": None,
                "district": None,
                "features": [],
                "property_description": ""
            }

            # Image URL
            try:
                img_el = self.driver.find_element(By.XPATH, '//link[@rel="preload" and @as="image"]')
                image_src = img_el.get_attribute("imagesrcset")
                if image_src:
                    data["image_url"] = image_src.split(',')[0].strip().split(' ')[0]
            except Exception:
                pass

            # Breadcrumbs
            try:
                script_elements = self.driver.find_elements(By.XPATH, '//script[@type="application/ld+json"]')
                for script_el in script_elements:
                    try:
                        json_data = json.loads(script_el.get_attribute("innerHTML"))
                        if isinstance(json_data, dict) and json_data.get("@type") == "BreadcrumbList":
                            for item in json_data.get("itemListElement", []):
                                if item.get("position") == 2:
                                    data["city"] = item.get("name")
                                elif item.get("position") == 3:
                                    data["district"] = item.get("name")
                            break
                    except Exception:
                        continue
            except Exception:
                pass

            # Features
            try:
                features = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="key-feature-item"]')))
                for ele in features:
                    try:
                        title_el = ele.find_element(By.XPATH, './/*[@id="item_title"]')
                        text_el = ele.find_element(By.XPATH, './/*[@id="key-feature-text"]')
                        title = title_el.text.strip() if title_el else None
                        text = text_el.text.strip() if text_el else None
                        if title and text:
                            data["features"].append(f"{title}: {text}")
                    except NoSuchElementException:
                        continue
            except Exception:
                pass

            # Description
            try:
                seo_title = self.driver.find_element(By.CSS_SELECTOR, 'span[data-testid="seo-title-meta"]').text
                seo_description = self.driver.find_element(By.CSS_SELECTOR, 'span[data-testid="seo-description-meta"]').text 

                li_elements = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, 'ul[aria-label="description-heading"] li')
                    )
                )
                content = [li.get_attribute("textContent").strip() for li in li_elements]
                
                xpath_selector = '//span[@aria-label="main-street-name-heading"]/ancestor::div[contains(@class, "text-om-t16")]'
                container = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, xpath_selector))
                )
                raw_text = container.get_attribute("textContent")

                parts = [
                        seo_title,
                        seo_description,
                        " ".join(content),   
                        raw_text
                    ]

                paragraph = " ".join(
                    part.strip() for part in parts if part and part.strip()
                )
                data["property_description"] = paragraph
            except Exception:
                pass
            
            self.log(f"Successfully extracted details for {url}", "DEBUG")
            return data
        except (TimeoutException, NoSuchElementException) as e:
            self.log(f"Selenium timeout or element not found for {url}: {e}", "WARN")
            return None

    def scrape_with_retries(self, url):
        for attempt in range(1, MAX_RETRIES + 1):
            if self.stop_requested.is_set(): 
                break
            result = self.extract_listing_details(url)
            if result: 
                return result
            time.sleep(RETRY_DELAY)
        return None

    def process_listings_from_json(self, json_path):
        if not os.path.exists(json_path):
            print(f"JSON file not found: {json_path}")
            return
        
        with open(json_path, "r", encoding="utf-8") as f:
            try:
                urls = json.load(f)
            except json.JSONDecodeError as e:
                self.log(f"Invalid JSON format: {e}", "ERROR")
                return

        for url in tqdm(urls, desc="Scraping details"):
            if self.stop_requested.is_set(): 
                break
            result = self.scrape_with_retries(url)
            if result:
                self.db.save_listing(result, self.fieldnames)

    def shutdown(self):
        self.log("Shutting down...", "INFO")
        self.stop_requested.set()
        if self.driver:
            self.driver.quit()
        self.db.close()