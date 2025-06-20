import re, json, os , time ,random , logging, hashlib, csv
import mysql.connector
from mysql.connector import Error
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, StaleElementReferenceException
)

load_dotenv()

class FacebookScraperLogger:
    def setup():
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("facebook_scraper.log", encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger("FacebookGroupScraper")

class BrowserManager:
    def get_random_user_agent():
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0 Safari/537.36"
        ]
        return random.choice(user_agents)

    def create_browser(headless=False):
        options = Options()
        if headless:
            options.add_argument("--headless")
        options.add_argument("--disable-notifications")
        options.add_argument("--window-size=720,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument(f"user-agent={BrowserManager.get_random_user_agent()}")
        return webdriver.Chrome(options=options)

class FacebookGroupScraper:
    def __init__(self, headless, cookies_file, config_file):
        self.logger = FacebookScraperLogger.setup()
        self.driver = BrowserManager.create_browser(headless)
        self.cookies_file = cookies_file
        self.config: Dict = {} 
        self.districts: List[str] = []
        self.wards: Dict[str, List[str]] = {}
        self.streets: List[str] = [] 
        self.amenity_patterns: Dict[str, str] = {}
        self.load_location_config(config_file)
        self.logger.info("Facebook group scraper initialized")
        self.db_connection = None
        self.db_cursor = None

    def print_header(self, config):
        print("\n" + "="*50)
        print(" FACEBOOK GROUP SCRAPER & DATABASE IMPORTER")
        print("="*50)
        print(f"• Groups to scrape: {len(config.get('groups', []))}")
        print(f"• Post limit per group: {config.get('max_posts', 0) if config.get('max_posts', 0) > 0 else 'No limit'}")
        print(f"• Output file: {config.get('csv_file_path', 'N/A')}")
        print(f"• Headless mode: {'On' if config.get('headless', False) else 'Off'}")
        print(f"• Import to database: {'Yes' if config.get('import_to_db', False) else 'No'}")
        print("="*50 + "\n")
        
    def load_location_config(self, config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            self.districts = self.config.get("districts", [])
            self.wards = self.config.get("wards", {})
            self.streets = self.config.get("streets", []) 
            self.amenity_patterns = self.config.get("amenity_patterns", {})
            self.logger.info(f"Loaded {len(self.districts)} districts, {len(self.wards)} ward mappings")
        except Exception as e:
            self.logger.error(f"Error loading config file: {e}")
            self.districts, self.wards,self.streets, self.amenity_patterns = [], [], [], {}

    def generate_content_hash(self, content):
        if not content:
            return ""
        normalized_content = ' '.join(content.lower().split())
        return hashlib.md5(normalized_content.encode('utf-8')).hexdigest()

    def load_cookies(self):
        try:
            with open(self.cookies_file, "r") as file:
                cookies = json.load(file)
            for cookie in cookies:
                self.driver.add_cookie(cookie)
            self.logger.info(f"Cookies loaded from {self.cookies_file}")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error(f"Cookie file error: {e}")

    def login(self):
        self.logger.info("Logging into Facebook...")
        self.driver.get("https://www.facebook.com/")
        WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        if self.cookies_file:
            self.load_cookies()
            self.driver.refresh()
        return self.verify_login_status()

    def verify_login_status(self):
        try:
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.XPATH, ".//input[@placeholder='Search Facebook']"))
            )
            self.logger.info("Login successful")
            return True
        except (TimeoutException, NoSuchElementException):
            self.logger.error("Login failed - search bar not found")
            return False

    def expand_post_content(self, post_element):
        try:
            see_more_buttons = post_element.find_elements(
                By.XPATH, ".//div[@role='button' and contains(text(), 'See more') or contains(text(), 'Xem thêm')]")
            for btn in see_more_buttons:
                self.driver.execute_script("arguments[0].click();", btn)
                time.sleep(0.5)
        except Exception as e:
            self.logger.warning(f"Failed to expand post: {e}")

    def extract_post_date(self, post_element):
        try:
            span_elem = post_element.find_element(By.XPATH,
                ".//span[@class='xmper1u xt0psk2 xjb2p0i x1qlqyl8 x15bjb6t x1n2onr6 x17ihmo5 x1g77sc7']"
            )
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", span_elem)
            
            date_elem = "div.x11i5rnm.x1mh8g0r.xexx8yu.x4uap5.x18d9i69.xkhd6sd.x78zum5.xjpr12u.xr9ek0c.x3ieub6.x6s0dn4"
            ActionChains(self.driver).move_to_element(span_elem).perform()
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, date_elem)))
            date_tooltip = self.driver.find_element(By.CSS_SELECTOR, date_elem)
            return self.format_date(date_tooltip.text.strip())
        
        except Exception as e:
            self.logger.warning(f"Failed to extract date: {e}")
            return ""

    def format_date(self, date_string):
        try:
            if 'at' not in date_string:
                return date_string
            date_part, time_part = date_string.split(' at ')
            date_words = date_part.split()
            day = date_words[-3].zfill(2)
            month_map = {
                'January': '01', 'February': '02', 'March': '03', 'April': '04',
                'May': '05', 'June': '06', 'July': '07', 'August': '08',
                'September': '09', 'October': '10', 'November': '11', 'December': '12'
            }
            month = month_map.get(date_words[-2], '01')
            year = date_words[-1]
            return f"{year}-{month}-{day} {time_part}:00"
        except Exception as e:
            self.logger.warning(f"Failed to format date '{date_string}': {e}")
            return date_string

    def extract_post_content(self, post_element):
        """Extract post content using multiple fallback methods."""
        selectors = [
            ".//div[@data-ad-rendering-role='story_message']",
            ".//div[contains(@class, 'x6s0dn4') and contains(@class, 'xh8yej3')]",
            ".//div[@class='xdj266r x11i5rnm xat24cr x1mh8g0r x1vvkbs x126k92a']"
        ]

        for selector in selectors:
            try:
                content_element = post_element.find_element(By.XPATH, selector)
                return content_element.text
            except NoSuchElementException:
                continue

        self.logger.error("No content extracted with any selector")
        return ""

    def _parse_price(self, content: str) -> int:
        try:
            content_lower = content.lower()
            
            match = re.search(r'(\d+[.,]?\d*)\s*(triệu|tr|tỷ|ty|trieu)\b', content_lower)
            if match:
                value = float(match.group(1).replace(',', '.'))
                unit = match.group(2)
                return int(value * (1_000_000_000 if unit in ['tỷ', 'ty'] else 1_000_000))
            
            match = re.search(r'(\d+)tr(\d)\b', content_lower)
            if match:
                value = float(f"{match.group(1)}.{match.group(2)}")
                return int(value * 1_000_000)
        except (ValueError, AttributeError):
            self.logger.warning("Could not parse price")
            return 0
        return 0

    def _parse_location(self, content: str) -> tuple[str, str]:
        if not content or not self.districts:
            return "", ""
        content_lower = content.lower()
        detected_district = next((d for d in self.districts if re.search(r"\b" + re.escape(d.lower()) + r"\b", content_lower)), "")
        detected_ward = ""
        if detected_district and detected_district in self.wards:
            detected_ward = next((w for w in self.wards[detected_district] if re.search(r"\b" + re.escape(w.lower()) + r"\b", content_lower)), "")
        return detected_district, detected_ward

    def _parse_amenities(self, content: str) -> str:
        if not content or not self.amenity_patterns:
            return ""
        content_lower = content.lower()
        amenities = {label for label, pattern in self.amenity_patterns.items() if re.search(pattern, content_lower, re.IGNORECASE)}
        return ", ".join(sorted(amenities))

    def _parse_area(self, content: str) -> str:
        if not content:
            return ""
        matches = re.findall(r'(\d+(?:\.\d+)?)\s*(?:m2|m²|met vuong)\b', content, re.IGNORECASE)
        return float(matches[0]) if matches else ""

    def _parse_address(self, content: str) -> str:
        if not content:
            return ""

        found_matches = []
        for street in self.streets:
            for match in re.finditer(r'\b(\d*\s*' + re.escape(street) + r'(?:\s+\d+)?)\b', content, re.IGNORECASE):
                found_matches.append(match)

        if found_matches:
            first_match = sorted(found_matches, key=lambda m: m.start())[0]
            return first_match.group(0).strip()

        return ""

    def _parse_contact(self, content: str) -> str:
        if not content:
            return ""
        pattern = r'\b(?:0|\+84)\d{1,2}[\s.-]?\d{3,4}[\s.-]?\d{3,4}\b'
        matches = re.findall(pattern, content, re.IGNORECASE)
        if not matches:
            return ""
        contact = matches[0].replace('O', '0').replace('o', '0')
        contact = re.sub(r'[^\d+]', '', contact)
        if contact.startswith('+84') and 9 <= len(contact[3:]) <= 11:
            return contact
        if contact.startswith('0') and 9 <= len(contact) <= 11:
            return contact
        if contact.startswith('84') and 9 <= len(contact[2:]) <= 11:
            return "0" + contact[2:]
        if 9 <= len(contact) <= 10:
            return "0" + contact
        return ""

    def parse_property_details(self, content):
        if not content:
            return {
                "area": "", "district": "", "ward": "", "address": "",
                "amenities": "", "price": 0, "contact": ""
            }
        price = self._parse_price(content)
        district, ward = self._parse_location(content)
        amenities = self._parse_amenities(content)
        area = self._parse_area(content)
        address = self._parse_address(content)
        contact = self._parse_contact(content)
        return {
            "area": area, "district": district, "ward": ward, "address": address,
            "amenities": amenities, "price": price, "contact": contact
        }

    def load_existing_csv_data(self, csv_file_path):
        existing_posts = []
        content_hashes = set()
        if os.path.exists(csv_file_path):
            try:
                with open(csv_file_path, 'r', encoding='utf-8', newline='') as f:
                    csv_reader = csv.DictReader(f)
                    for row in csv_reader:
                        existing_posts.append(row)
                        content_hashes.add(row.get("postID", ""))
            except Exception as e:
                self.logger.error(f"Error loading CSV: {e}")
        return existing_posts, content_hashes

    def scrape_group_posts(self, group_url, max_posts, csv_file_path):
        self.logger.info(f"Scraping group: {group_url}")
        self.driver.get(group_url)
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, ".//div[@class='x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z']")))
        except TimeoutException:
            self.logger.error("Posts did not load")
            return 0

        csv_columns = ["postID", "postDate", "content", "area", "district", "ward", "address", "amenities", "price", "contact"]
        all_posts, content_hashes = self.load_existing_csv_data(csv_file_path)
        posts_scraped = 0

        while posts_scraped < max_posts:
            post_elements = self.driver.find_elements(By.XPATH, ".//div[@class='x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z']")
            new_posts = 0

            for post in post_elements:
                if posts_scraped >= max_posts:
                    break
                try:
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", post)
                    self.expand_post_content(post)
                    content = self.extract_post_content(post)
                    content_hash = self.generate_content_hash(content)
                    if content_hash in content_hashes:
                        continue
                    content_hashes.add(content_hash)
                    post_date = self.extract_post_date(post)
                    property_details = self.parse_property_details(content)
                    all_posts.append({
                        "postID": content_hash, "postDate": post_date, "content": content,
                        **property_details
                    })
                    posts_scraped += 1
                    new_posts += 1
                    self.logger.info(f"Scraped post {posts_scraped}/{max_posts}")
                    time.sleep(random.uniform(1, 2))
                except Exception as e:
                    self.logger.warning(f"Error scraping post: {e}")
                    continue

            if not new_posts:
                break
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            try:
                WebDriverWait(self.driver, 5).until(
                    lambda d: len(d.find_elements(By.XPATH, ".//div[@class='x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z']")) > len(post_elements))
            except TimeoutException:
                break

        try:
            with open(csv_file_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=csv_columns)
                writer.writeheader()
                writer.writerows(all_posts)
            self.logger.info(f"Saved {len(all_posts)} posts to {csv_file_path}")
        except Exception as e:
            self.logger.error(f"Error saving CSV: {e}")

        return posts_scraped, all_posts

    def connect_to_db(self):
        """Connect to the MySQL database."""
        try:
            self.db_connection = mysql.connector.connect(
                host=os.getenv('db_host'),
                user=os.getenv('db_user'),
                password=os.getenv('db_password'),
                database=os.getenv('db_name'),
                connection_timeout=10
            )
            self.db_connection.autocommit = False
            self.db_cursor = self.db_connection.cursor()
            self.logger.info("Connected to database")
            return True
        except Error as e:
            self.logger.error(f"Database connection error: {str(e)}")
            return False

    def close_db_connection(self):
        """Close the database connection if it's open."""
        if self.db_connection and self.db_connection.is_connected():
            if self.db_cursor:
                self.db_cursor.close()
            self.db_connection.close()
            self.logger.info("Database connection closed")

    def import_to_database(self, data: List[Dict[str, Any]], batch_size: int = 100) -> bool:
        """Import data to MySQL database with upsert (replace if exists)."""
        if not data:
            self.logger.warning("No data to import to database")
            return False
            
        if not self.connect_to_db():
            return False
            
        try:
            # Create table if it doesn't exist
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
            self.logger.info("Table 'post' checked/created")
            
            # Prepare SQL for upserting
            upsert_sql = """
                INSERT INTO post (
                    postID, p_date, content, district, ward,
                    street_address, price, area, amenities, contact_info
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    p_date = VALUES(p_date), content = VALUES(content), district = VALUES(district),
                    ward = VALUES(ward), street_address = VALUES(street_address), price = VALUES(price),
                    area = VALUES(area), amenities = VALUES(amenities), contact_info = VALUES(contact_info)
            """
            
            records_processed = 0
            # Process data in batches
            for i, row in enumerate(data):
                # Convert amenities string to JSON array string
                amenities_list = row["amenities"].split(', ') if isinstance(row["amenities"], str) and row["amenities"] else []
                amenities_json = json.dumps(amenities_list, ensure_ascii=False)
                
                # Prepare values tuple, mapping FB data keys to DB columns
                values = (
                    row["postID"], row["postDate"], row["content"], row["district"],
                    row["ward"], row["address"], row["price"], row["area"],
                    amenities_json, row["contact"]
                )
                
                self.db_cursor.execute(upsert_sql, values)
                records_processed += 1
                
                # Commit every batch_size records
                if (i + 1) % batch_size == 0:
                    self.db_connection.commit()
                    self.logger.info(f"Committed batch of {batch_size} records.")

            # Final commit for any remaining records
            self.db_connection.commit()
            self.logger.info(f"Database import complete. Total records processed: {records_processed}")
            return True
            
        except Error as e:
            self.logger.error(f"Database error: {str(e)}")
            self.db_connection.rollback()
            return False
        finally:
            self.close_db_connection()
    
    def close(self):
        try:
            self.driver.quit()
            self.logger.info("Browser closed")
        except Exception:
            self.logger.info("No browser instance to close")

def main():
    headless = False
    cookies_file = "facebook_cookies.json"
    config_file = "config.json"
    max_posts = 5
    csv_file_path = 'scrapData.csv'
    groups = ["https://www.facebook.com/groups/281184089051767"]

    import_to_db = False
    db_batch_size = 100
    
    config_dict = {
        "groups": groups,
        "max_posts": max_posts,
        "csv_file_path": csv_file_path,
        "headless": headless,
        "import_to_db": import_to_db
    }
    
    scraper = FacebookGroupScraper(headless, cookies_file, config_file)
    scraper.print_header(config_dict)
    start_time = time.time()
    
    try:
        if not scraper.login():
            logging.error("Login failed")
            return
        
        all_scraped_data = []
        for group_url in groups:
            posts_scraped, posts_data = scraper.scrape_group_posts(group_url, max_posts, csv_file_path)
            scraper.logger.info(f"Scraped {posts_scraped} posts from {group_url}")
            all_scraped_data.extend(posts_data[-posts_scraped:] if posts_scraped > 0 else [])

        if import_to_db and all_scraped_data:
            scraper.logger.info(f"Importing {len(all_scraped_data)} posts to database...")
            scraper.import_to_database(all_scraped_data, db_batch_size)
        elif import_to_db:
            scraper.logger.info("No new data to import to database.")
            
    except Exception as e:
        logging.error(f"Script error: {e}")
    finally:
        scraper.close()
        print(f"⏱️ Total execution time: {time.time() - start_time:.2f} seconds")
        
if __name__ == "__main__":
    main()