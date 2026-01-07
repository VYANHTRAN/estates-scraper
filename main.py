import argparse
import sys
import asyncio

from src.scraping_utils import Scraper
from src.cleaning_utils import DataCleaner
from src.config import *


async def run_scrape_urls():
    scraper = Scraper()
    await scraper.init_browser()

    print("[INFO] Scraping listing URLs...")
    try:
        urls = scraper.scrape_menu_pages()
        scraper.save_urls(urls)
    finally:
        await scraper.shutdown()


async def run_scrape_details():
    scraper = Scraper()
    await scraper.init_browser()

    print("[INFO] Scraping listing details from saved URLs...")
    try:
        await scraper.process_listings_from_json(URLS_OUTPUT_PATH)
    finally:
        await scraper.shutdown()


def run_clean_data():
    print("[INFO] Cleaning scraped data...")
    cleaner = DataCleaner()
    cleaner.load_data()
    cleaner.clean_data()
    cleaner.save_cleaned_data()
    print("[INFO] Data cleaning completed successfully.")


async def run_full_pipeline():
    print("[INFO] Running full scraping and cleaning pipeline...")
    await run_scrape_urls()
    await run_scrape_details()
    run_clean_data()
    print("[INFO] Full pipeline completed.")


def main():
    parser = argparse.ArgumentParser(description="Run scraping and cleaning tasks.")
    parser.add_argument(
        "task",
        choices=["scrape_urls", "scrape_details", "clean_data", "full_pipeline"],
    )
    args = parser.parse_args()

    if args.task == "scrape_urls":
        asyncio.run(run_scrape_urls())
    elif args.task == "scrape_details":
        asyncio.run(run_scrape_details())
    elif args.task == "clean_data":
        run_clean_data()
    elif args.task == "full_pipeline":
        asyncio.run(run_full_pipeline())


if __name__ == "__main__":
    main()