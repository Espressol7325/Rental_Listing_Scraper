## 🏠 **Real Estate Scraper**  

This project is a robust web scraping tool designed to collect and process real estate rental listings. It uses Selenium to automate data extraction and export the results to CSV and MySQL for further analysis.

## 📦 **Features**  

-  🔍 Scrapes rental listing data including title, price, area, location, and amenities

-  📍 Detects district and ward names using regex-based text extraction

-  💰 Extracts price (in VND) and area (in m²) even from free-text titles

-  🧠 Classifies listing types (room, apartment, house, etc.)

- 🗂️ Outputs results to .csv

- 🔧 Customizable via config.json

## 🛠️ **Tech Stack**  

- Python 3.10+

-  Selenium Webdriver

- re (regular expressions)

-  csv

## 📁 **File Structure**   

├── scrapScript.ipynb       # Main Jupyter notebook to run the scraper  
├── config.json             # Configuration file (URL list, stop words, regex for location/amenity)  
├── phongtro_data.json      # Scraped data in JSON format  
├── phongtro_data.csv       # Scraped data in CSV format  

## ⚙️ **Configuration**

The `config.json` file allows you to specify:

- **URLs** to scrape
- **Regex patterns** for:
  - **District and ward name extraction**
  - **Amenity detection**
  - **Type classification** (e.g., apartment, shared room)


## 🚀 **How to Run**

1.  Clone the repository:   
git clone https://github.com/Espressol7325/Rental_Listing_Scraper.git
cd phongtro-scraper  

2.  Install dependencies:  
pip install -r requirements.txt  

3.  Launch the scraper:  
Run Scrapping_FB.py or Scrapping_Web.py depending on which method you want to get data; or 
4.  Output will be saved as:  
phongtro_data.csv  

## 📊 **Use Cases**  
-  Data analysis in Power BI or Excel

-  Training datasets for machine learning

-  Real estate market research

## 📌 **Notes**
-  You must have EdgeDriver installed and in your system PATH.

-  Scraping respects pagination and attempts to avoid duplicate entries.

-  Some listings may have incomplete or noisy data; basic cleaning is included.
