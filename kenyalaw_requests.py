import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from datetime import datetime
import time
import re
import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("scraper_requests.log"),
        logging.StreamHandler()
    ]
)

# Disable verbose logs from other libraries
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

# Initialize S3 client
bucket_name = "denningdata"
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
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Create a session with retries and proper headers
def create_session():
    session = requests.Session()
    
    # Set up retries
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))
    
    # Set headers to mimic a real browser
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    })
    
    return session

# Check AWS credentials
def check_aws_credentials():
    try:
        s3.list_buckets()
        return True
    except NoCredentialsError:
        logging.error("AWS credentials not found")
        return False

base_url = "https://new.kenyalaw.org"

def get_page(session, url, timeout=30):
    """Get a page with error handling"""
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        
        # Add small delay to be respectful
        time.sleep(0.5)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get {url}: {e}")
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
                    full_url = urljoin(base_url, a_tag["href"])
                    links.append(full_url)
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

def extract_document_link(session, url):
    try:
        page = get_page(session, url)
        if not page:
            return None
        
        # Look for download links - try multiple selectors
        selectors = [
            "a.btn.btn-primary.btn-shrink-sm",
            "a[href*='/source']",
            "a[href$='.pdf']",
            "a[href$='.doc']",
            "a[href$='.docx']"
        ]
        
        for selector in selectors:
            elements = page.select(selector)
            for element in elements:
                href = element.get('href')
                if href:
                    if href.startswith('/'):
                        href = urljoin(base_url, href)
                    
                    # Check if it's a valid document link
                    if (href.lower().endswith(('.pdf', '.doc', '.docx')) or 
                        '/source' in href.lower()):
                        text = element.get_text(strip=True)
                        if is_document_size_greater_than_zero(text):
                            return href
        
        # Fallback: Look in dd elements
        dd_elements = page.find_all("dd")
        if dd_elements:
            for dd in reversed(dd_elements):  # Start from the last one
                a_tag = dd.find("a")
                if a_tag and "href" in a_tag.attrs:
                    href = a_tag["href"]
                    if href.startswith('/'):
                        href = urljoin(base_url, href)
                    
                    if (href.lower().endswith(('.pdf', '.doc', '.docx')) or 
                        '/source' in href.lower()):
                        return href
        
        return None
    except Exception as e:
        logging.error(f"Error extracting document link from {url}: {e}")
        return None

def download_document_to_s3(url, folder="documents"):
    try:
        # Check if already processed
        with processed_urls_lock:
            if url in processed_urls:
                return None
            processed_urls.add(url)
        
        parsed_url = urlparse(url)
        
        # Extract meaningful filename from Kenya Law URLs
        if '/source' in url and 'kenyalaw.org' in url:
            path_parts = parsed_url.path.split('/')
            if len(path_parts) >= 6:
                court = path_parts[4]
                year = path_parts[5]
                case_id = path_parts[6]
                filename = f"{court}_{year}_{case_id}.pdf"
            else:
                filename = f"document_{int(time.time())}.pdf"
        else:
            filename = os.path.basename(parsed_url.path) or f"document_{int(time.time())}"
            if not filename.lower().endswith(('.pdf', '.doc', '.docx')):
                filename += ".pdf"
        
        now = datetime.now()
        s3_key = f"{folder}/{now.year}/{now.month:02d}/{filename}"
        
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
        session = create_session()
        response = session.get(url, timeout=30)
        
        if response.status_code == 200:
            if len(response.content) == 0:
                logging.warning(f"Empty file: {url}")
                return None
                
            # Upload to S3
            s3.put_object(Bucket=bucket_name, Key=s3_key, Body=response.content)
            logging.info(f"✓ Uploaded: {filename} ({len(response.content)} bytes)")
            return f"s3://{bucket_name}/{s3_key}"
        else:
            logging.error(f"Download failed ({response.status_code}): {url}")
            return None
            
    except Exception as e:
        logging.error(f"S3 upload error for {url}: {e}")
        return None

def process_single_document(link):
    """Process a single document link"""
    try:
        session = create_session()
        document_link = extract_document_link(session, link)
        if document_link:
            s3_link = download_document_to_s3(document_link)
            return s3_link
        return None
    except Exception as e:
        logging.error(f"Error processing {link}: {e}")
        return None

def extract_all_cases_links_in_a_query(session, url):
    all_alphabets_links = extract_alphabetical_links(url)
    all_documents = []
    
    for i, alphabet_link in enumerate(all_alphabets_links, 1):
        if cleanup_initiated:
            break
            
        logging.info(f"Processing alphabet {i}/26...")
        page_1 = get_page(session, alphabet_link)
        if not page_1:
            continue
            
        pages_links = extract_page_numbers_links(alphabet_link, page_1)
        
        # Collect all PDF links first
        pdf_download_page_links = []
        for page_link in pages_links:
            if cleanup_initiated:
                break
            page_2 = get_page(session, page_link)
            if not page_2:
                continue
            pdf_download_page_links.extend(pdf_links(page_2))
        
        # Process PDF links with limited concurrency
        if pdf_download_page_links:
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_link = {executor.submit(process_single_document, link): link 
                                for link in pdf_download_page_links}
                
                for future in as_completed(future_to_link):
                    if cleanup_initiated:
                        break
                    try:
                        result = future.result()
                        if result:
                            all_documents.append(result)
                    except Exception as e:
                        link = future_to_link[future]
                        logging.error(f"Error processing {link}: {e}")
                        
    return all_documents

def final_page_scrapper(url):
    all_downloadable_links = set()
    document_count = 0
    
    try:
        logging.info("Starting requests-based scraper...")
        session = create_session()
        
        scraped_page = get_page(session, url)
        if not scraped_page:
            logging.error(f"Failed to scrape base URL: {url}")
            return all_downloadable_links
            
        years_links = years_links_extract(url, scraped_page)
        
        for year_idx, year_link in enumerate(years_links, 1):
            if cleanup_initiated:
                break
                
            logging.info(f"Processing year {year_idx}/{len(years_links)}...")
            year_page = get_page(session, year_link)
            if not year_page:
                continue
                
            months_links = months_links_extract(year_link, year_page)
            
            for month_idx, month_link in enumerate(months_links, 1):
                if cleanup_initiated:
                    break
                    
                logging.info(f"  Month {month_idx}/{len(months_links)}")
                downloadable_links = extract_all_cases_links_in_a_query(session, month_link)
                
                for link in downloadable_links:
                    if link not in all_downloadable_links:
                        all_downloadable_links.add(link)
                        document_count += 1
                        
        # Fallback for direct scraping
        if not cleanup_initiated and (not years_links or years_links == [url]):
            logging.info("No year structure found, scraping directly...")
            downloadable_links = extract_all_cases_links_in_a_query(session, url)
            for link in downloadable_links:
                if link not in all_downloadable_links:
                    all_downloadable_links.add(link)
                    document_count += 1
                    
    except Exception as e:
        logging.error(f"Script failed: {e}")
    
    logging.info(f"COMPLETED: {document_count} documents collected")
    
    # Save results
    try:
        with open("s3_links_requests.txt", "w") as f:
            for link in all_downloadable_links:
                f.write(f"{link}\n")
    except Exception as e:
        logging.error(f"Error saving results: {e}")
            
    return all_downloadable_links

if __name__ == "__main__":
    # Check AWS credentials first
    if not check_aws_credentials():
        exit(1)
    
    try:
        target_url = "https://new.kenyalaw.org/judgments/court-class/superior-courts/"
        result = final_page_scrapper(target_url)
        logging.info(f"Final result: {len(result)} documents")
        
    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
    except Exception as e:
        logging.error(f"Script failed: {e}")