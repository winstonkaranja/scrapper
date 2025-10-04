from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from datetime import datetime
import time
import re
import requests
import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue, Empty
import psutil
import signal
import sys
import tempfile
import shutil

# Configure logging - ONLY IMPORTANT STUFF
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

# Disable verbose logs from other libraries
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

# Initialize S3 client
bucket_name = "kenya-law-denning"
s3 = boto3.client('s3')

# Track processed URLs to avoid duplicates
processed_urls = set()
processed_urls_lock = threading.Lock()

# Global cleanup flag
cleanup_initiated = False

def signal_handler(sig, frame):
    global cleanup_initiated
    if not cleanup_initiated:
        cleanup_initiated = True
        logging.info("Shutting down gracefully...")
        cleanup_all_resources()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def kill_existing_chrome():
    """Kill any existing Chrome processes more aggressively"""
    try:
        # Kill chrome processes
        os.system("pkill -f chrome")
        os.system("pkill -f chromedriver")
        time.sleep(3)
        
        # Force kill if still running
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['name'] and ('chrome' in proc.info['name'].lower() or 'chromedriver' in proc.info['name'].lower()):
                    proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        # Clean up temp directories
        temp_dirs = [d for d in os.listdir('/tmp') if d.startswith('chrome_user_data_')]
        for temp_dir in temp_dirs:
            try:
                shutil.rmtree(f'/tmp/{temp_dir}')
            except:
                pass
                
        time.sleep(2)
    except Exception as e:
        logging.warning(f"Error in chrome cleanup: {e}")

def initialize_driver(attempt=0):
    """Initialize a single WebDriver with better error handling"""
    temp_dir = None
    try:
        # Create unique temp directory with timestamp for better uniqueness
        import uuid
        temp_dir = tempfile.mkdtemp(
            prefix=f'chrome_user_data_{os.getpid()}_{attempt}_{uuid.uuid4().hex[:8]}_',
            dir='/tmp'
        )

        options = Options()
        options.add_argument("--headless=new")  # Use new headless mode
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-backgrounding-occluded-windows")
        options.add_argument("--disable-renderer-backgrounding")
        options.add_argument("--disable-default-apps")
        options.add_argument("--no-first-run")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument(f"--user-data-dir={temp_dir}")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")

        # Block images and media for faster loading
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_settings.popups": 0,
            "profile.managed_default_content_settings.media_stream": 2,
        }
        options.add_experimental_option("prefs", prefs)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(30)
        driver.implicitly_wait(5)

        # Test the driver with a simple page
        driver.get("data:text/html,<html><body>Test</body></html>")
        time.sleep(0.5)

        # Store temp directory for cleanup
        driver._temp_dir = temp_dir

        return driver

    except Exception as e:
        logging.error(f"Failed to initialize driver (attempt {attempt}): {e}")
        # Clean up temp directory if created
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception as cleanup_error:
                logging.warning(f"Failed to cleanup temp dir {temp_dir}: {cleanup_error}")
        return None

def cleanup_driver(driver):
    """Safely cleanup a single driver"""
    if driver:
        try:
            # Clean up temp directory
            if hasattr(driver, '_temp_dir'):
                temp_dir = driver._temp_dir
            else:
                temp_dir = None
                
            driver.quit()
            
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                
        except Exception as e:
            logging.warning(f"Error cleaning up driver: {e}")

# Simplified driver pool with better resource management
class DriverPool:
    def __init__(self, size=1):  # Start with just 1 driver
        self.drivers = Queue()
        self.lock = threading.Lock()
        self.active_drivers = []
        self.max_size = size
        
        # Create initial driver
        driver = self.create_new_driver()
        if driver:
            self.drivers.put(driver)
            self.active_drivers.append(driver)
        else:
            logging.error("Failed to create initial driver")
    
    def create_new_driver(self):
        """Create a new driver with retries"""
        for attempt in range(3):
            if attempt > 0:
                logging.info(f"Retrying driver creation (attempt {attempt + 1}/3)...")
                # Kill any leftover chrome processes before retry
                kill_existing_chrome()
                time.sleep(3)
            driver = initialize_driver(attempt)
            if driver:
                return driver
        return None
    
    def get_driver(self, timeout=30):
        """Get a driver from the pool or create a new one"""
        try:
            # Try to get from queue first
            return self.drivers.get(timeout=5)
        except Empty:
            # If queue is empty and we haven't hit max size, create new driver
            with self.lock:
                if len(self.active_drivers) < self.max_size:
                    driver = self.create_new_driver()
                    if driver:
                        self.active_drivers.append(driver)
                        return driver
            
            # Wait for a driver to be returned
            try:
                return self.drivers.get(timeout=timeout)
            except Empty:
                logging.error("No drivers available after timeout")
                return None
    
    def return_driver(self, driver):
        """Return a driver to the pool if it's still working"""
        if driver:
            try:
                # Test if driver is still responsive
                driver.current_url
                self.drivers.put(driver)
            except Exception as e:
                logging.warning(f"Driver failed test, removing: {e}")
                self.cleanup_single_driver(driver)
    
    def cleanup_single_driver(self, driver):
        """Clean up a single driver"""
        try:
            if driver in self.active_drivers:
                self.active_drivers.remove(driver)
            cleanup_driver(driver)
        except Exception as e:
            logging.warning(f"Error cleaning up single driver: {e}")
    
    def cleanup(self):
        """Clean up all drivers"""
        # Clear the queue first
        while not self.drivers.empty():
            try:
                driver = self.drivers.get_nowait()
                cleanup_driver(driver)
            except Empty:
                break
            except Exception as e:
                logging.warning(f"Error cleaning up queued driver: {e}")
        
        # Clean up active drivers
        for driver in self.active_drivers[:]:  # Copy list to avoid modification during iteration
            cleanup_driver(driver)
        
        self.active_drivers.clear()

# Check AWS credentials
def check_aws_credentials():
    try:
        s3.list_buckets()
        return True
    except NoCredentialsError:
        logging.error("AWS credentials not found")
        return False

base_url = "https://new.kenyalaw.org"

def safe_driver_operation(func, driver, *args, **kwargs):
    """Wrapper for safe driver operations with automatic retry"""
    max_retries = 2
    for attempt in range(max_retries):
        try:
            return func(driver, *args, **kwargs)
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Driver operation failed, retrying: {e}")
                time.sleep(1)
            else:
                logging.error(f"Driver operation failed after {max_retries} attempts: {e}")
                raise

def scrape_page(driver, url, retries=2):
    """Scrape a page with better error handling"""
    for attempt in range(retries):
        try:
            driver.get(url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(1)  # Small delay to ensure page is loaded
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            return soup
        except Exception as e:
            if attempt < retries - 1:
                logging.warning(f"Retrying {url} due to: {e}")
                time.sleep(2)
            else:
                logging.error(f"Failed to scrape {url} after {retries} attempts: {e}")
                return None
    return None

def years_links_extract(url, page):
    try:
        ul_elements = page.find_all("ul", class_="year-nav mb-0 ms-2")
        if not ul_elements:
            return [url]
        year_links = []
        for li in ul_elements[0].find_all("li"):
            a_tag = li.find("a")
            if a_tag and "href" in a_tag.attrs:
                year = a_tag["href"].split("/")[-2]
                year_links.append(url + year + "/")
        logging.info(f"Found {len(year_links)} years")
        return year_links
    except Exception as e:
        logging.error(f"Error extracting year links: {e}")
        return [url]

def months_links_extract(url, page):
    try:
        ul_elements = page.find_all("ul", class_="year-nav mb-0 ms-2")
        if len(ul_elements) >= 2:
            months_links = []
            for li in ul_elements[1].find_all("li"):
                a_tag = li.find("a")
                if a_tag and "href" in a_tag.attrs:
                    month = a_tag["href"].split("/")[-2]
                    months_links.append(url + month + "/")
            return months_links
        else:
            return [url]
    except Exception as e:
        logging.error(f"Error extracting month links: {e}")
        return [url]

def extract_page_numbers_links(url, page):
    try:
        ul_element = page.find("ul", class_="pagination flex-wrap")
        if not ul_element:
            return [url]
        page_numbers = []
        for li in ul_element.find_all("li"):
            a_tag = li.find("a")
            if a_tag and "href" in a_tag.attrs:
                page_numbers.append(f"{url}&{a_tag['href'][12:]}")
        return list(set(page_numbers))
    except Exception as e:
        logging.error(f"Error extracting page numbers: {e}")
        return [url]

def extract_alphabetical_links(url):
    return [f"{url}?alphabet={chr(i)}" for i in range(ord('a'), ord('z') + 1)]

def pdf_links(page):
    try:
        tr_elements = page.find_all("tr")
        links = []
        for tr in tr_elements:
            td_title = tr.find("td", class_="cell-title")
            if td_title:
                a_tag = td_title.find("a")
                if a_tag and "href" in a_tag.attrs:
                    links.append(base_url + a_tag["href"])
        return links
    except Exception as e:
        logging.error(f"Error extracting case links: {e}")
        return []

def is_document_size_greater_than_zero(text):
    if not text:
        return True
    match = re.search(r'(\d+(\.\d+)?)\s*(KB|MB)', text, re.IGNORECASE)
    if match:
        size = float(match.group(1))
        unit = match.group(3).upper()
        return size > 0 if unit == "KB" else size > 0.001
    return True

def extract_document_link(driver, url):
    try:
        soup = scrape_page(driver, url)
        if not soup:
            return None
        
        # First try: Look for primary download button
        try:
            a_element = driver.find_element(By.CSS_SELECTOR, "a.btn.btn-primary.btn-shrink-sm")
            a_tag = a_element.get_attribute("href")
            if a_tag and (a_tag.lower().endswith(('.pdf', '.doc', '.docx')) or '/source' in a_tag.lower()) and is_document_size_greater_than_zero(a_element.text.strip()):
                return a_tag
        except Exception:
            pass
        
        # Second try: Look in dd elements
        dd_elements = soup.find_all("dd")
        if dd_elements:
            a_tag = dd_elements[-1].find("a")
            if a_tag and "href" in a_tag.attrs and (a_tag["href"].lower().endswith(('.pdf', '.doc', '.docx')) or '/source' in a_tag["href"].lower()):
                return a_tag["href"]
        
        return None
    except Exception as e:
        logging.error(f"Error extracting document link from {url}: {e}")
        return None

def download_document_to_s3(url, year_name, month_name, folder="documents"):
    """Download document to S3 with proper year/month folder structure and metadata"""
    try:
        # Check if already processed
        with processed_urls_lock:
            if url in processed_urls:
                return None
            processed_urls.add(url)

        parsed_url = urlparse(url)

        # Extract meaningful filename and metadata from Kenya Law URLs
        if '/source' in url and 'kenyalaw.org' in url:
            path_parts = parsed_url.path.split('/')
            if len(path_parts) >= 6:
                court = path_parts[4]
                url_year = path_parts[5]
                case_id = path_parts[6]
                # Create comprehensive filename with court, year, and case ID
                filename = f"{court}_{url_year}_{case_id}.pdf"
            else:
                filename = f"document_{int(time.time())}.pdf"
        else:
            filename = os.path.basename(parsed_url.path) or f"document_{int(time.time())}"
            if not filename.lower().endswith(('.pdf', '.doc', '.docx')):
                filename += ".pdf"

        # Use comprehensive folder naming: documents/2024/January/ instead of documents/2024/01/
        s3_key = f"{folder}/{year_name}/{month_name}/{filename}"

        # Check if file already exists
        try:
            s3.head_object(Bucket=bucket_name, Key=s3_key)
            return f"s3://{bucket_name}/{s3_key}"  # File exists, return link
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code not in ['404', '403']:
                logging.error(f"S3 error: {e}")
                return None

        # Download the document
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = session.get(url, timeout=30, headers=headers)

        if response.status_code == 200:
            if len(response.content) == 0:
                logging.warning(f"Empty file: {url}")
                return None

            # Upload to S3
            s3.put_object(Bucket=bucket_name, Key=s3_key, Body=response.content)
            logging.info(f"✓ Uploaded: {year_name}/{month_name}/{filename} ({len(response.content)} bytes)")
            return f"s3://{bucket_name}/{s3_key}"
        else:
            logging.error(f"Download failed ({response.status_code}): {url}")
            return None

    except Exception as e:
        logging.error(f"S3 upload error for {url}: {e}")
        return None

def process_single_document(link, year_name, month_name):
    """Process a single document link with its own driver"""
    driver = None
    try:
        driver = driver_pool.get_driver()
        if not driver:
            logging.error(f"No driver available for {link}")
            return None

        document_link = extract_document_link(driver, link)
        if document_link:
            s3_link = download_document_to_s3(document_link, year_name, month_name)
            return s3_link
        return None
    except Exception as e:
        logging.error(f"Error processing {link}: {e}")
        return None
    finally:
        if driver:
            driver_pool.return_driver(driver)

def extract_all_cases_links_in_a_query(driver, url, year_name, month_name):
    """Extract all cases for a given year and month, iterating through alphabets"""
    all_alphabets_links = extract_alphabetical_links(url)
    all_documents = []

    for i, alphabet_link in enumerate(all_alphabets_links, 1):
        if cleanup_initiated:
            break

        letter = chr(ord('a') + i - 1)
        logging.info(f"    [{year_name}/{month_name}] Processing letter '{letter}' ({i}/26)...")
        page_1 = scrape_page(driver, alphabet_link)
        if not page_1:
            continue

        pages_links = extract_page_numbers_links(alphabet_link, page_1)

        # Collect all PDF links first
        pdf_download_page_links = []
        for page_link in pages_links:
            if cleanup_initiated:
                break
            page_2 = scrape_page(driver, page_link)
            if not page_2:
                continue
            pdf_download_page_links.extend(pdf_links(page_2))

        # Process PDF links sequentially, passing year and month context
        if pdf_download_page_links:
            for link in pdf_download_page_links:
                if cleanup_initiated:
                    break
                result = process_single_document(link, year_name, month_name)
                if result:
                    all_documents.append(result)

    return all_documents

def final_page_scrapper(driver, url):
    """Main scraper that filters by year → month → alphabet"""
    all_downloadable_links = set()
    document_count = 0

    # Month number to name mapping
    month_names = {
        '1': 'January', '2': 'February', '3': 'March', '4': 'April',
        '5': 'May', '6': 'June', '7': 'July', '8': 'August',
        '9': 'September', '10': 'October', '11': 'November', '12': 'December'
    }

    try:
        logging.info("Starting scraper with year → month → alphabet filtering...")
        scraped_page = scrape_page(driver, url)
        if not scraped_page:
            logging.error(f"Failed to scrape base URL: {url}")
            return all_downloadable_links

        years_links = years_links_extract(url, scraped_page)

        for year_idx, year_link in enumerate(years_links, 1):
            if cleanup_initiated:
                break

            # Extract year name from URL (e.g., "2024" from ".../2024/")
            year_name = year_link.rstrip('/').split('/')[-1]
            logging.info(f"[{year_idx}/{len(years_links)}] Processing Year: {year_name}")

            year_page = scrape_page(driver, year_link)
            if not year_page:
                continue

            months_links = months_links_extract(year_link, year_page)

            for month_idx, month_link in enumerate(months_links, 1):
                if cleanup_initiated:
                    break

                # Extract month number from URL and convert to name
                month_num = month_link.rstrip('/').split('/')[-1]
                month_name = month_names.get(month_num, month_num)
                logging.info(f"  [{year_name}] Processing Month: {month_name} ({month_idx}/{len(months_links)})")

                # Pass year and month context down the chain
                downloadable_links = extract_all_cases_links_in_a_query(driver, month_link, year_name, month_name)

                for link in downloadable_links:
                    if link not in all_downloadable_links:
                        all_downloadable_links.add(link)
                        document_count += 1

        # Fallback for direct scraping (when no year structure exists)
        if not cleanup_initiated and (not years_links or years_links == [url]):
            logging.info("No year structure found, scraping directly...")
            # Use current date as fallback for naming
            now = datetime.now()
            year_name = str(now.year)
            month_name = now.strftime('%B')
            downloadable_links = extract_all_cases_links_in_a_query(driver, url, year_name, month_name)
            for link in downloadable_links:
                if link not in all_downloadable_links:
                    all_downloadable_links.add(link)
                    document_count += 1

    except Exception as e:
        logging.error(f"Script failed: {e}")

    logging.info(f"COMPLETED: {document_count} documents collected")

    # Save results
    try:
        with open("s3_links.txt", "w") as f:
            for link in all_downloadable_links:
                f.write(f"{link}\n")
    except Exception as e:
        logging.error(f"Error saving results: {e}")

    return all_downloadable_links

def cleanup_all_resources():
    """Clean up all resources"""
    global driver_pool
    
    if 'driver_pool' in globals():
        driver_pool.cleanup()
    
    kill_existing_chrome()

if __name__ == "__main__":
    # Check AWS credentials first
    if not check_aws_credentials():
        exit(1)
    
    # Kill any existing Chrome processes
    kill_existing_chrome()
    
    driver = None
    
    try:
        # Initialize driver pool with just 1 driver
        driver_pool = DriverPool(size=1)
        
        # Initialize main driver
        driver = initialize_driver()
        if not driver:
            logging.error("Failed to initialize main WebDriver")
            exit(1)
        
        target_url = "https://new.kenyalaw.org/judgments/court-class/superior-courts/"
        result = final_page_scrapper(driver, target_url)
        logging.info(f"Final result: {len(result)} documents")
        
    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
    except Exception as e:
        logging.error(f"Script failed: {e}")
    finally:
        cleanup_initiated = True
        if driver:
            cleanup_driver(driver)
        cleanup_all_resources()