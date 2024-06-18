import os
import csv
import json
import logging
from urllib.parse import urlencode
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

from selenium import webdriver
from selenium.webdriver.common.by import By

OPTIONS = webdriver.ChromeOptions()
OPTIONS.add_argument("--headless")

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_search_results(keyword, location, page_number, data_pipeline=None, retries=3):

    driver = webdriver.Chrome(options=OPTIONS)

    formatted_keyword = keyword.replace(" ", "+")
    url = f"https://www.trustpilot.com/search?query={formatted_keyword}&page={page_number+1}"

    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            driver.get(url)
            logger.info(f"{keyword}: Fetched page {page_number}")
            
                ## Extract Data
            script_tag = driver.find_element(By.CSS_SELECTOR, "script[id='__NEXT_DATA__'")
            if script_tag:
                json_data = json.loads(script_tag.get_attribute("innerHTML"))

                
                business_units = json_data["props"]["pageProps"]["businessUnits"]

                for business in business_units:

                    name = business.get("displayName").lower().replace(" ", "").replace("'", "")
                    trustpilot_formatted = business.get("contact")["website"].split("://")[1]
                    location = business.get("location")
                    category_list = business.get("categories")
                    category = category_list[0]["categoryId"] if len(category_list) > 0 else "n/a"

                    ## Extract Data
                    search_data = {
                        "name": business.get("displayName", ""),
                        "stars": business.get("stars", 0),
                        "rating": business.get("trustScore", 0),
                        "num_reviews": business.get("numberOfReviews", 0),
                        "website": business.get("contact")["website"],
                        "trustpilot_url": f"https://www.trustpilot.com/review/{trustpilot_formatted}",
                        "location": location.get("country", "n/a"),
                        "category": category
                    }

                    print(search_data)
                logger.info(f"Successfully parsed data from: {url}")

                driver.quit()
                success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


def start_scrape(keyword, pages, location, data_pipeline=None, retries=3):
    for page in range(pages):
        scrape_search_results(keyword, page, location, data_pipeline, retries=retries)


if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    PAGES = 1
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["online bank"]

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        crawl_pipeline = DataPipeline(csv_filename=f"{filename}.csv")
        start_scrape(keyword, PAGES, LOCATION, data_pipeline=crawl_pipeline, max_threads=MAX_THREADS, retries=MAX_RETRIES)
        crawl_pipeline.close_pipeline()
    logger.info(f"Crawl complete.")