import os
import json
import time
import random
import sys
import threading 
import sqlite3

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, SessionNotCreatedException

from src.config import *


class DriverPool:
    def __init__(self, user_agent_generator):
        self.driver = None
        self.ua = user_agent_generator
        self._scraper_stop_requested = threading.Event() 
        self._init_driver()

    def _init_driver(self):
        self.log("Initializing WebDriver...", "INFO")
        user_agent = self._get_random_user_agent()
        options = Options()
        options.add_argument(f"user-agent={user_agent}")
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--enable-unsafe-swiftshader")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument("--window-size=1920,1080")

        try:
            self.driver = webdriver.Chrome(options=options)
            self.log("Successfully initialized WebDriver.", "DEBUG")
        except SessionNotCreatedException as e:
            self.log(f"[ERROR] Could not create a Chrome session. Ensure chromedriver matches your Chrome version. Error: {e}", "CRITICAL")
            raise RuntimeError("Failed to initialize WebDriver.") from e
        except Exception as e:
            self.log(f"[ERROR] An unexpected error occurred while creating a WebDriver: {e}", "CRITICAL")
            raise RuntimeError("Failed to initialize WebDriver.") from e

    def _get_random_user_agent(self):
        try:
            return self.ua.random
        except Exception:
            fallback_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
                "Mozilla/5.0 (Windows NT 10.0; rv:115.0) Gecko/20100101 Firefox/115.0"
            ]
            return random.choice(fallback_agents)

    def acquire(self):
        if self._scraper_stop_requested.is_set():
            raise RuntimeError("Acquire cancelled: Scraper shutdown initiated.")
        if self.driver and self.driver.session_id:
            return self.driver
        else:
            self.log("Driver is not active, attempting to re-initialize.", "WARN")
            self._init_driver()
            return self.driver


    def release(self, driver):
        pass

    def close_all(self):
        self.log("Closing WebDriver instance...", "INFO")
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except Exception as e:
                self.log(f"[ERROR] Error quitting WebDriver: {e}", "WARN")
        self.log("WebDriver instance closed.", "INFO")

    def log(self, message, level="INFO"):
        if hasattr(self, '_scraper_log_method'):
            self._scraper_log_method(message, level)
        else:
            levels = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
            config_level = LOG_LEVEL.upper() if 'LOG_LEVEL' in globals() else "INFO"
            config_level_idx = levels.index(config_level)
            message_level_idx = levels.index(level.upper())
            if message_level_idx >= config_level_idx:
                print(f"[DriverPool-{level}] {message}", file=sys.stderr if level in ["ERROR", "CRITICAL"] else sys.stdout)


class Scraper:
    def __init__(self):
        self.ua = UserAgent()
        self.driver_pool = DriverPool(self.ua)
        self.driver_pool._scraper_log_method = self.log
        self.stop_requested = threading.Event() 
        self.driver_pool._scraper_stop_requested = self.stop_requested 

        self.all_scraped_urls = set()
        self.fieldnames = [
            "listing_title", "property_id", "total_price",
            "unit_price", "property_url", "image_url",
            "city", "district", "alley_width",
            "features", "property_description"
        ]
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Database Initialization 
        self.db_lock = threading.Lock()
        self.conn = None
        self._init_db()

    def log(self, message, level="INFO"):
        levels = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
        config_level_idx = levels.index(LOG_LEVEL.upper()) if LOG_LEVEL.upper() in levels else 1
        message_level_idx = levels.index(level.upper()) if level.upper() in levels else 1
        if message_level_idx >= config_level_idx:
            print(f"[{level}] {message}", file=sys.stderr if level in ["ERROR", "CRITICAL"] else sys.stdout)

    # -------------------- Database Methods --------------------
    def _init_db(self):
        """Initializes SQLite database and creates the table if not exists."""
        try:
            # check_same_thread=False is needed if connection is shared (though we use a lock)
            self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
            cursor = self.conn.cursor()
            
            # Create table. property_id is the PRIMARY KEY to ensure uniqueness.
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id TEXT,
                listing_title TEXT,
                total_price TEXT,
                unit_price TEXT,
                property_url TEXT,
                image_url TEXT,
                city TEXT,
                district TEXT,
                alley_width TEXT,
                features TEXT,
                property_description TEXT,
                has_updated INTEGER DEFAULT 0, 
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_table_sql)

             # Create an index on property_id for faster lookups
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_property_id ON listings (property_id);")

            self.conn.commit()
            self.log(f"Database initialized at {DB_PATH}", "INFO")
        except Exception as e:
            self.log(f"Failed to initialize database: {e}", "CRITICAL")
            sys.exit(1)

    def _close_db(self):
        with self.db_lock:
            if self.conn:
                self.conn.close()
                self.conn = None
                self.log("Database connection closed.", "INFO")

    def save_details_to_db(self, listing):
        """
        Inserts listing details into SQLite as a new version.
        Updates old versions to flag them as updated.
        """
        if not listing or not self.conn:
            return

        # Prepare data: Join lists into strings 
        listing_copy = listing.copy()
        if isinstance(listing_copy.get("features"), list):
            listing_copy["features"] = "; ".join(listing_copy.get("features", []))
        if isinstance(listing_copy.get("property_description"), list):
            listing_copy["property_description"] = ". ".join(listing_copy.get("property_description", []))

        # Filter to ensure we only insert known fields
        data_to_insert = {k: v for k, v in listing_copy.items() if k in self.fieldnames}
        
        property_id = data_to_insert.get("property_id")
        # Ensure property_id exists
        if not property_id:
            self.log(f"Skipping listing without property_id: {data_to_insert.get('property_url')}", "WARN")
            return

        with self.db_lock:
            try:
                cursor = self.conn.cursor()
                
                # Check if this property_id already exists in the DB
                cursor.execute("SELECT COUNT(*) FROM listings WHERE property_id = ?", (property_id,))
                count = cursor.fetchone()[0]
                
                has_updated_flag = 0
                
                if count > 0:
                    # Logic: If data exists, we flag ALL versions (old and this new one) as 'has_updated = 1'
                    # 1. Update existing records
                    cursor.execute("UPDATE listings SET has_updated = 1 WHERE property_id = ?", (property_id,))
                    # 2. Set flag for new record
                    has_updated_flag = 1
                    self.log(f"Updating history for property {property_id}. New version added.", "DEBUG")
                
                # Insert new record (Always INSERT, never replace, to keep history)
                query = """
                INSERT INTO listings (
                    listing_title, property_id, total_price, unit_price, 
                    property_url, image_url, city, district, alley_width, 
                    features, property_description, has_updated, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """
                
                values = (
                    data_to_insert.get("listing_title"),
                    data_to_insert.get("property_id"),
                    data_to_insert.get("total_price"),
                    data_to_insert.get("unit_price"),
                    data_to_insert.get("property_url"),
                    data_to_insert.get("image_url"),
                    data_to_insert.get("city"),
                    data_to_insert.get("district"),
                    data_to_insert.get("alley_width"),
                    data_to_insert.get("features"),
                    data_to_insert.get("property_description"),
                    has_updated_flag 
                )
                
                cursor.execute(query, values)
                self.conn.commit()
                
            except Exception as e:
                self.log(f"Error writing to DB: {e} with data: {property_id}", "ERROR")


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
        for page_num in tqdm(range(1, TOTAL_PAGES + 1), desc="Scraping menu pages"):
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
                    headers = {"User-Agent": self.driver_pool._get_random_user_agent()}
                    response = requests.get(url, headers=headers, timeout=10)

                    if 400 <= response.status_code < 600:
                        consecutive_http_errors += 1
                        if consecutive_http_errors >= 3:
                            self.log(f"Critical: {consecutive_http_errors} HTTP errors at {url}. Stopping.", "CRITICAL")
                            self.stop_requested.set()
                            break
                        raise Exception(f"Status code {response.status_code}")
                    elif not response.text:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= 3:
                            self.log(f"Critical: {consecutive_empty_pages} empty pages at {url}. Stopping.", "CRITICAL")
                            self.stop_requested.set()
                            break
                        raise Exception("Empty page content")

                    consecutive_http_errors = 0
                    consecutive_empty_pages = 0
                    links = self.get_listing_urls(response.text)
                    self.log(f"Extracted {len(links)} links from page {page_num}", "DEBUG")
                    self.all_scraped_urls.update(links)
                    break
                except requests.exceptions.RequestException as e:
                    self.log(f"Retry {retry + 1}/{MAX_RETRIES} for page {page_num}: {e}", "WARN")
                    if self.stop_requested.is_set():
                        break
                    time.sleep(RETRY_DELAY)
                except Exception as e:
                    self.log(f"Retry {retry + 1}/{MAX_RETRIES} for page {page_num}: {e}", "WARN")
                    if self.stop_requested.is_set():
                        break
                    time.sleep(RETRY_DELAY)
            else:
                self.log(f"Failed to fetch page {page_num} after {MAX_RETRIES} retries.", "ERROR")

            if self.stop_requested.is_set():
                break
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
        if self.stop_requested.is_set():
            self.log(f"Skipping {url} as shutdown is requested.", "DEBUG")
            return None
        
        driver = None
        try:
            driver = self.driver_pool.acquire() # Acquire the single driver
            
            if self.stop_requested.is_set():
                self.log(f"Shutdown requested immediately after acquiring driver for {url}.", "DEBUG")
                return None

            driver.get(url)
            wait = WebDriverWait(driver, 10)

            wait.until(EC.presence_of_element_located((By.XPATH, "/html/body")))

            def safe_text(by, selector, timeout=5):
                try:
                    return wait.until(EC.presence_of_element_located((by, selector))).text.strip()
                except (TimeoutException, NoSuchElementException, WebDriverException):
                    return None

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
                "property_description": []
            }

            # Image URL
            try:
                img_el = driver.find_element(By.XPATH, '//link[@rel="preload" and @as="image"]')
                image_src = img_el.get_attribute("imagesrcset")
                if image_src:
                    data["image_url"] = image_src.split(',')[0].strip().split(' ')[0]
            except Exception:
                pass

            # Breadcrumbs
            try:
                script_elements = driver.find_elements(By.XPATH, '//script[@type="application/ld+json"]')
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
                desc_div = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="property-description"]')
                if desc_div and desc_div.text:
                    data["property_description"] = [desc_div.text.strip()]
                else:
                    desc_elements = wait.until(EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, 'ul[aria-label="description-heading"].relative li')
                    ))
                    data["property_description"] = [
                        li.text.strip() for li in desc_elements if li.text.strip()
                    ]
            except Exception:
                pass
            
            self.log(f"Successfully extracted details for {url}", "DEBUG")
            return data
        except (TimeoutException, NoSuchElementException) as e:
            self.log(f"Selenium timeout or element not found for {url}: {e}", "WARN")
            return None
        except WebDriverException as e:
            self.log(f"WebDriver error for {url}: {e}. Attempting to re-initialize driver.", "ERROR")
            try:
                if self.driver_pool.driver:
                    self.driver_pool.driver.quit()
                    self.driver_pool.driver = None 
            except Exception as ex:
                self.log(f"Error quitting driver after WebDriverException for {url}: {ex}", "ERROR")
            return None
        except RuntimeError as e:
            self.log(f"Scraping for {url} cancelled during driver acquisition: {e}", "INFO")
            return None
        except Exception as e:
            self.log(f"An unexpected error occurred while scraping {url}: {e}", "ERROR")
            return None
        finally:
            if driver:
                self.driver_pool.release(driver)

    def scrape_with_retries(self, url):
        if self.stop_requested.is_set():
            self.log(f"Stopping retries for {url} as shutdown is requested.", "DEBUG")
            return None
        
        for attempt in range(1, MAX_RETRIES + 1):
            if self.stop_requested.is_set():
                self.log(f"Stopping retries for {url} in attempt {attempt} as shutdown is requested.", "DEBUG")
                return None
            try:
                result = self.extract_listing_details(url)
                if result is not None:
                    return result
            except RuntimeError:
                self.log(f"Scraping for {url} cancelled during retry attempt {attempt} due to shutdown.", "INFO")
                return None
            except Exception as e:
                self.log(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}", "WARN")
                if attempt < MAX_RETRIES and not self.stop_requested.is_set():
                    time.sleep(RETRY_DELAY)
        self.log(f"All {MAX_RETRIES} attempts failed for {url}.", "ERROR")
        return None

    def process_listings_from_json(self, json_path, output_path_dummy=None):
        """
        Loads URLs, scrapes them, and upserts into SQLite.
        """
        # 1. Load listing URLs
        if not os.path.exists(json_path):
            self.log(f"JSON file not found: {json_path}", "ERROR")
            return
        
        with open(json_path, "r", encoding="utf-8") as f:
            try:
                urls = json.load(f)
            except json.JSONDecodeError as e:
                self.log(f"Invalid JSON format: {e}", "ERROR")
                return

        if not urls:
            self.log("No URLs found in JSON.", "INFO")
            return

        self.log(f"Starting to scrape {len(urls)} listings into SQLite...", "INFO")
        
        try:
            for url in tqdm(urls, desc="Scraping listing details"):
                if self.stop_requested.is_set():
                    self.log("Shutdown requested.", "INFO")
                    break

                result = self.scrape_with_retries(url)

                if result:
                    # Save to DB (Handles Update if property_id exists, Insert otherwise)
                    self.save_details_to_db(result)
                
        except KeyboardInterrupt:
            self.log("KeyboardInterrupt detected.", "INFO")
            self.stop_requested.set()
        except Exception as e:
            self.log(f"Unexpected error: {e}", "CRITICAL")
            self.stop_requested.set()
        finally:
            self.shutdown()


    def shutdown(self):
        self.log("Shutting down scraper components...", "INFO")
        self.stop_requested.set()
        self.driver_pool.close_all()
        self._close_db()
        self.log("Scraper shutdown complete.", "INFO")