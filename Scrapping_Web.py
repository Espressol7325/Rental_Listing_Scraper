import re, json, os , time ,random , logging, hashlib, csv
import smtplib
from email.message import EmailMessage
import mysql.connector
from mysql.connector import Error
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, StaleElementReferenceException
)


# ======== CONFIGURATION ========
DEFAULT_CONFIG = {
    "city": "da-nang",                      # City to scrape data from (URL path)
    "post_limit": 5,                        # Number of posts to scrape (0 = all)
    "output_file": "Scraped_data.csv",      # Output filename
    "headless": True,                       # Run browser in headless mode (True) or visible (False)
    "random_delay": True,                   # Add random delay between operations (to avoid blocking)
    "min_delay": 1,                         # Minimum delay (seconds)
    "max_delay": 3,                         # Maximum delay (seconds)
    "import_to_db": True,                   # Import data to database after scraping
    "db_batch_size": 100,                   # Number of records to commit in each batch
    "db_retry_limit": 3,                    # Number of retries for database operations
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w',
    filename='data_scraper.log'
)
logger = logging.getLogger(__name__)


class DateScraper:
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize scraper with configuration."""
        self.config = config or DEFAULT_CONFIG
        self.driver = None
        self.patterns = self._load_config()
        self.db_connection = None
        self.db_cursor = None
        
    def _load_config(self) -> Dict:
        """Load patterns and location data from config.json file."""
        try:
            with open('config.json', 'r', encoding='utf-8') as config_file:
                return json.load(config_file)
        except Exception as e:
            logger.error(f"Error loading config.json: {str(e)}")
            return {}
    
    def setup_driver(self) -> webdriver.Edge:
        """Set up and return WebDriver instance."""
        options = Options()
        if self.config["headless"]:
            options.add_argument("--headless")  
            options.add_argument("--disable-gpu")
        
        options.add_argument("--window-size=720,1080")
        options.add_argument("--disable-notifications")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        # Add user-agent to avoid detection as bot
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
        
        self.driver = webdriver.Edge(options=options)
        return self.driver

    def random_delay(self) -> float:
        """Create random delay if configured."""
        if self.config["random_delay"]:
            delay = random.uniform(self.config["min_delay"], self.config["max_delay"])
            time.sleep(delay)
            return delay
        return 0

    def check_and_move_to_next_page(self) -> bool:
        """Check and move to next page"""
        try:
            next_button = self.driver.find_element(By.XPATH, "//a[text()='Trang sau »']")
            if next_button.is_enabled():
                next_button.click()
                delay = self.random_delay()
                logger.info(f"Moved to next page (waited {delay:.2f}s)")
                return True
            else:
                logger.info("'Next' button not available or not found.")
                return False
        except NoSuchElementException:
            logger.info("'Next' button not found on this page.")
            return False
        except Exception as e:
            logger.error(f"Error moving to next page: {str(e)}")
            return False

    def get_all_urls(self, max_posts: int = 0) -> List[str]:
        """Get all post URLs from the website, limit if specified."""
        all_post_url = []
        current_page = 1
        
        while True:
            try:
                logger.info(f"Getting URLs from page {current_page}")
                
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@class,'line-clamp-2')]"))
                )
                
                post_elements = self.driver.find_elements(By.XPATH, "//a[contains(@class,'line-clamp-2')]")
                
                for element in post_elements:
                    url = element.get_attribute('href')
                    all_post_url.append(url)
                    logger.debug(f"Added URL: {url}")
                    
                    if max_posts > 0 and len(all_post_url) >= max_posts:
                        logger.info(f"Reached limit of {max_posts} posts.")
                        return all_post_url[:max_posts]
                
                logger.info(f"Collected {len(all_post_url)} URLs")
                
                if not self.check_and_move_to_next_page():
                    break
                    
                current_page += 1
                
            except Exception as e:
                logger.error(f"Error getting URLs: {str(e)}")
                break
        
        return all_post_url

    def extract_datetime(self, date_time_str: str) -> str:
        """Extract date and time from string and format it as 'YYYY-MM-DD HH:MM:SS'."""
        try:
            parts = date_time_str.split(', ')
            if len(parts) < 2:
                return ""

            raw_datetime = parts[1] 
            dt = datetime.strptime(raw_datetime, "%H:%M %d/%m/%Y")
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        except Exception as e:
            logger.error(f"Error extracting date and time: {str(e)}")
            return ""

    def get_post_content(self) -> str:
        """Get post content from description."""
        try:
            # Wait for element to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='border-bottom pb-3 mb-4']"))
            )
            
            paragraphs = self.driver.find_elements(By.XPATH, "//div[@class='border-bottom pb-3 mb-4']/p")
            
            post_content_paragraphs = [p.text.strip() for p in paragraphs]
            post_content = "\n".join(post_content_paragraphs)
            return post_content.strip()  

        except TimeoutException:
            logger.warning("Timeout waiting for description element")
            return ""
        except Exception as e:
            logger.error(f"Error getting post content: {str(e)}")
            return ""

    def generate_post_id(self, content: str) -> str:
        """Generate unique ID from post content."""
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    def get_district_and_ward(self, address: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract district and ward from address string using keyword matching."""
        if not address or not self.patterns:
            return None, None

        try:
            # Get district list from config
            districts = self.patterns.get("districts", [])
            wards = self.patterns.get("wards", {})

            detected_district = None
            for district in districts:
                if re.search(r"\b" + re.escape(district) + r"\b", address, re.IGNORECASE):
                    detected_district = district
                    break

            detected_ward = None
            if detected_district and detected_district in wards:
                for ward in wards[detected_district]:
                    if re.search(r"\b" + re.escape(ward) + r"\b", address, re.IGNORECASE):
                        detected_ward = ward
                        break

            # Fall back to simple substring match if regex fails
            if not detected_ward and detected_district:
                address_lower = address.lower()
                for ward in wards[detected_district]:
                    if ward.lower() in address_lower:
                        detected_ward = ward
                        break

            return detected_district, detected_ward

        except Exception as e:
            logger.error(f"Error parsing address '{address}': {str(e)}")
            return None, None
        
    def get_amenities(self, content: str) -> List[str]:
        """Get amenities list from post."""
        if not self.patterns:
            return []
        # Get amenity patterns from config
        amenity_patterns = self.patterns.get("amenity_patterns", {})
        detected_amenities = set()
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((
                    By.XPATH,
                    "//div[@class='text-body d-flex pt-1 pb-1' and not(contains(@style, '--bs-text-opacity: 0.1;'))]")))
            amenity_elements = self.driver.find_elements(
                By.XPATH,
                "//div[@class='text-body d-flex pt-1 pb-1' and not(contains(@style, '--bs-text-opacity: 0.1;'))]")
            for element in amenity_elements:
                text = element.text.strip()
                if text:
                    matched = False
                    for label, pattern in amenity_patterns.items():
                        if re.search(pattern, text, re.IGNORECASE):
                            detected_amenities.add(label)
                            matched = True
                            break
                    if not matched:
                        detected_amenities.add(text) 
            # Get from content
            for label, pattern in amenity_patterns.items():
                if re.search(pattern, content, re.IGNORECASE):
                    detected_amenities.add(label)
            return list(detected_amenities)
        except TimeoutException:
            logger.warning("Timeout waiting for amenity elements")
            return list(detected_amenities)
        except Exception as e:
            logger.error(f"Error getting amenities: {str(e)}")
            return list(detected_amenities)

    def extract_price_value(self, price_str: str) -> Optional[int]:
        """Extract numeric value from price string and return as integer (VND)."""
        try:
            s = price_str.lower().replace('đồng', '').replace('vnd', '').replace('/tháng', '').strip()

            if m := re.search(r'(\d+)[.,](\d+)\s*triệu', s):
                return int(m.group(1)) * 1_000_000 + int(m.group(2).ljust(2, '0')) * 10_000
            elif m := re.search(r'(\d+)\s*triệu', s):
                return int(m.group(1)) * 1_000_000
            elif m := re.search(r'(\d{3,}(?:[.,]\d{3})*)', s):
                return int(m.group(1).replace('.', '').replace(',', ''))

            return None
        except Exception as e:
            logger.error(f"Error processing price: {e}")
            return None
    
    def extract_area_value(self, area_str: str) -> Optional[float | int]:
        """Extract numeric value from area string."""
        try:
            match = re.search(r'([\d.,]+)', area_str)
            if not match:
                return None

            number = float(match.group(1).replace(',', '.'))
            return number
        except Exception as e:
            logger.error(f"Error processing area: {str(e)}")
            return None

        
    def get_element_text_safely(self, xpath: str, default: str = "") -> str:
        """Safely get text from an element with fallback."""
        try:
            element = self.driver.find_element(By.XPATH, xpath)
            return element.text.strip()
        except (NoSuchElementException, StaleElementReferenceException):
            return default
        
    def extract_metadata(self) -> Dict[str, Any]:
        raw = self.get_element_text_safely(
            "(//td[@class='border-0 pb-0'])[2]",
            self.get_element_text_safely("(//table[@class='table table-borderless align-middle m-0'])/tbody//tr[5]")
        )
        return {
            "time": self.extract_datetime(raw)
        }

    def extract_address_and_location(self) -> Dict[str, Any]:
        address = self.get_element_text_safely(
            "(//td[@colspan='3'])[3]",
            self.get_element_text_safely("(//table[@class='table table-borderless align-middle m-0'])/tbody//tr[3]/td[2]")
        )
        district, ward = self.get_district_and_ward(address)
        return {
            "address": address,
            "district": district,
            "ward": ward
        }

    def extract_info_area(self, content: str) -> Dict[str, Any]:
        price_str = self.get_element_text_safely(
            "//span[@class='text-price fs-5 fw-bold']",
            self.get_element_text_safely("//span[@class='text-green fs-5 fw-bold']")
        )
        area_str = self.get_element_text_safely("//div[@class='d-flex justify-content-between']/div/span[3]")
        return {
            "price": self.extract_price_value(price_str),
            "area": self.extract_area_value(area_str),
            "amenities": self.get_amenities(content)
        }

    def extract_contact(self) -> str:
        return self.get_element_text_safely(
            "//div[@class='mb-4']//i[@class='icon telephone-fill white me-2']/.."
        ).strip()

    def get_post_data(self, url: str) -> Optional[Dict[str, Any]]:
        """Get post data from URL by extracting parts separately."""
        try:
            self.driver.get(url)
            delay = self.random_delay()
            logger.info(f"Loading page {url} (waited {delay:.2f}s)")

            if "Page not found" in self.driver.title or "Error" in self.driver.title:
                logger.warning(f"Page doesn't exist or has error: {url}")
                return None

            content = self.get_post_content()
            post_id = self.generate_post_id(content)

            metadata = self.extract_metadata()
            address_data = self.extract_address_and_location()
            pricing = self.extract_info_area(content)
            contact = self.extract_contact()

            return {
                "postID": post_id,
                "time": metadata["time"],
                "content": content,
                "address": address_data["address"],
                "ward": address_data["ward"],
                "district": address_data["district"],
                "area": pricing["area"],
                "price": pricing["price"],
                "amenities": pricing["amenities"],
                "contact": contact,
            }

        except Exception as e:
            logger.error(f"Error getting data from URL {url}: {str(e)}")
            return None

    def save_to_csv(self, data: List[Dict], filename: str) -> bool:
        """Save data to CSV file."""
        try:
            if not data:
                logger.warning("No data to save to CSV.")
                return False
                
            # Get field names from the first item
            fieldnames = list(data[0].keys())
            
            with open(filename, 'w', encoding='utf-8', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for row in data:
                    row_copy = row.copy()
                    if isinstance(row_copy["amenities"], list):
                        row_copy["amenities"] = json.dumps(row_copy["amenities"], ensure_ascii=False)
                    writer.writerow(row)
            
            logger.info(f"Saved data to {filename}")
            return True
        except Exception as e:
            logger.error(f"Error saving data to CSV file: {str(e)}")
            return False

    def print_summary(self, post_data_list: List[Dict]):
        """Print summary of collected data."""
        if not post_data_list:
            print("No data collected!")
            return
            
        print("\n" + "="*50)
        print(f"🏠 PHONGTRO DATA COLLECTION SUMMARY 🏠")
        print("="*50)
        print(f"✅ Number of posts collected: {len(post_data_list)}")
        
        # District stats
        districts = {}
        for post in post_data_list:
            district = post.get("district", "")
            if district:
                districts[district] = districts.get(district, 0) + 1
        
        if districts:
            print("\n📍 Distribution by district:")
            for district, count in sorted(districts.items(), key=lambda x: x[1], reverse=True):
                print(f"  • District {district}: {count} posts")
        
        # Price stats
        prices = [post.get("price", "") for post in post_data_list if post.get("price")]
        if prices:
            print("\n💰 Price information:")
            print(f"  • Number of posts with price info: {len(prices)}")
        
        print("\n💾 Data saved to: " + self.config["output_file"])
        
        if self.config["import_to_db"]:
            print("📊 Data imported to database")
            
        print("="*50 + "\n")

    def collect_posts(self, urls: List[str]) -> List[Dict[str, Any]]:
        posts = []
        for i, url in enumerate(urls):
            print(f"Processing post {i+1}/{len(urls)}", end='\r')
            logger.info(f"Processing {i+1}/{len(urls)}: {url}")
            data = self.get_post_data(url)
            if data:
                posts.append(data)
        return posts

    def connect_to_db(self):
        """Connect to the MySQL database."""
        try:
            load_dotenv()
            
            self.db_connection = mysql.connector.connect(
                host=os.getenv('db_host'),
                user=os.getenv('db_user'),
                password=os.getenv('db_password'),
                database=os.getenv('db_name'),
                connection_timeout=10
            )
            self.db_connection.autocommit = False  # Disable autocommit for batch processing
            self.db_cursor = self.db_connection.cursor()
            logger.info("Connected to database")
            return True
        except Error as e:
            logger.error(f"Database connection error: {str(e)}")
            print(f"Database connection error: {str(e)}")
            return False

    def close_db_connection(self):
        """Close the database connection if it's open."""
        if self.db_connection and self.db_connection.is_connected():
            if self.db_cursor:
                self.db_cursor.close()
            self.db_connection.close()
            logger.info("Database connection closed")
            print("Database connection closed")

    def import_to_database(self, data: List[Dict[str, Any]]) -> bool:
        """Import data to MySQL database with upsert (replace if exists)."""
        if not data:
            logger.warning("No data to import to database")
            return False
            
        # Connect to database
        if not self.connect_to_db():
            return False
            
        try:
            # Check if table exists, if not create it
            try:
                self.db_cursor.execute("""
                    CREATE TABLE IF NOT EXISTS post (
                        postID VARCHAR(32) PRIMARY KEY,
                        p_date DATETIME,
                        content LONGTEXT,
                        district VARCHAR(255),
                        ward VARCHAR(255),
                        street_address TEXT,
                        price INT,
                        area FLOAT,
                        amenities JSON,
                        contact_info VARCHAR(255),
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                """)
                self.db_connection.commit()
                logger.info("Table 'post' checked/created")
            except Error as e:
                logger.error(f"Error creating table: {str(e)}")
                return False
                
            # Prepare SQL for inserting or updating
            upsert_sql = """
                INSERT INTO post (
                    postID, p_date, content, district, ward,
                    street_address, price, area, amenities, contact_info
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    p_date = VALUES(p_date),
                    content = VALUES(content),
                    district = VALUES(district),
                    ward = VALUES(ward),
                    street_address = VALUES(street_address),
                    price = VALUES(price),
                    area = VALUES(area),
                    amenities = VALUES(amenities),
                    contact_info = VALUES(contact_info)
            """
            
            records_processed = 0
            records_updated = 0
            records_inserted = 0
            
            # Process data in batches
            for i, row in enumerate(data):
                try:
                    # Check if post already exists
                    check_sql = "SELECT COUNT(*) FROM post WHERE postID = %s"
                    self.db_cursor.execute(check_sql, (row["postID"],))
                    exists = self.db_cursor.fetchone()[0] > 0
                    
                    amenities_json = json.dumps(row["amenities"], ensure_ascii=False) if isinstance(row["amenities"], list) else row["amenities"]
                    
                    # Prepare values tuple
                    values = (
                        row["postID"],
                        row["time"],
                        row["content"],
                        row["district"],
                        row["ward"],
                        row["address"],
                        row["price"],
                        row["area"],
                        amenities_json,
                        row["contact"]
                    )
                    
                    # Try to insert/update with retries
                    for attempt in range(self.config["db_retry_limit"]):
                        try:
                            self.db_cursor.execute(upsert_sql, values)
                            records_processed += 1
                            
                            if exists:
                                records_updated += 1
                            else:
                                records_inserted += 1
                                
                            break 
                        except mysql.connector.errors.DatabaseError as e:
                            if "Lock wait timeout exceeded" in str(e) and attempt < self.config["db_retry_limit"] - 1:
                                logger.warning(f"Lock timeout on row {i}, retrying ({attempt + 1}/{self.config['db_retry_limit']})...")
                                time.sleep(2)
                            else:
                                raise
                    
                    # Commit every batch_size records
                    if i % self.config["db_batch_size"] == 0 and i > 0:
                        self.db_connection.commit()
                        logger.info(f"Committed batch of {self.config['db_batch_size']} records (total: {records_processed})")
                        print(f"Processed {records_processed} records ({records_inserted} new, {records_updated} updated)")
                
                except Exception as e:
                    logger.error(f"Error processing row {i}: {str(e)}")
                    print(f"Error processing row {i}: {str(e)}")
            
            # Final commit for remaining records
            self.db_connection.commit()
            logger.info(f"Database import complete. Total: {records_processed} records ({records_inserted} new, {records_updated} updated)")
            print(f"Database import complete. Total: {records_processed} records ({records_inserted} new, {records_updated} updated)")
            return True
            
        except Error as e:
            logger.error(f"Database error: {str(e)}")
            print(f"Database error: {str(e)}")
            return False
            
        finally:
            self.close_db_connection()
    def send_log_via_email(self, logfile: str, subject: str = "Scraper Log"):
            """Send the log file to your email address."""
            load_dotenv()
            EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
            EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
            EMAIL_TO = os.getenv("EMAIL_TO", EMAIL_ADDRESS)

            if not EMAIL_ADDRESS or not EMAIL_PASSWORD:
                logger.error("Email credentials not set in .env file.")
                return

            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = EMAIL_ADDRESS
            msg["To"] = EMAIL_TO
            msg.set_content("Attached is the latest scraper log file.")

            try:
                with open(logfile, "rb") as f:
                    msg.add_attachment(f.read(), maintype="text", subtype="plain", filename=logfile)
                with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                    smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
                    smtp.send_message(msg)
                logger.info("Log file sent via email.")
            except Exception as e:
                logger.error(f"Failed to send log via email: {str(e)}")
    def send_csv_via_email(self, csvfile: str = None, subject: str = "Scraped Data CSV"):
        """Send the scraped CSV file to your email address."""
        load_dotenv()
        EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
        EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
        EMAIL_TO = os.getenv("EMAIL_TO", EMAIL_ADDRESS)

        if not EMAIL_ADDRESS or not EMAIL_PASSWORD:
            logger.error("Email credentials not set in .env file.")
            return

        if not csvfile:
            csvfile = self.config.get("output_file", "Scraped_data.csv")

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = EMAIL_ADDRESS
        msg["To"] = EMAIL_TO
        msg.set_content("Attached is the latest scraped data CSV file.")

        try:
            with open(csvfile, "rb") as f:
                msg.add_attachment(f.read(), maintype="text", subtype="csv", filename=csvfile)
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
                smtp.send_message(msg)
            logger.info("CSV file sent via email.")
        except Exception as e:
            logger.error(f"Failed to send CSV via email: {str(e)}")
    def run(self):
        """Run the complete workflow: scrape data, save to CSV, and import to database."""
        print("Scraper started...")
        self.print_header()
        if not self.patterns:
            print("Error: Could not load config.json.")
            return

        start_time = time.time()
        self.setup_driver()

        try:
            self.driver.get(f"https://phongtro123.com/tinh-thanh/{self.config['city']}?orderby=moi-nhat")
            self.random_delay()

            urls = self.get_all_urls(self.config["post_limit"])
            posts = self.collect_posts(urls)

            if posts:
                # Save to CSV
                self.save_to_csv(posts, self.config["output_file"])
                
                # Import to database if configured
                if self.config["import_to_db"]:
                    try:
                        self.import_to_database(posts)
                    except Exception as e:
                        import traceback
                        print(f"[❌ DB IMPORT ERROR] {traceback.format_exc()}")
                            
                        self.print_summary(posts)
                    else:
                        print("No data collected.")
        finally:
            if self.driver:
                self.driver.quit()
            print(f"⏱️ Execution time: {time.time() - start_time:.2f} seconds")
            self.send_csv_via_email(self.config["output_file"])
            self.send_log_via_email('data_scraper.log') 

    def print_header(self):
        """Print program header."""
        print("\n" + "="*50)
        print("🏠 PHONGTRO DATA SCRAPER & DATABASE IMPORTER 🏠")
        print("="*50)
        print(f"• City: {self.config['city']}")
        print(f"• Post limit: {self.config['post_limit'] if self.config['post_limit'] > 0 else 'No limit'}")
        print(f"• Output file: {self.config['output_file']}")
        print(f"• Headless mode: {'On' if self.config['headless'] else 'Off'}")
        print(f"• Import to database: {'Yes' if self.config['import_to_db'] else 'No'}")
        print("="*50 + "\n")


if __name__ == "__main__":
    scraper = DateScraper(DEFAULT_CONFIG)
    scraper.run()