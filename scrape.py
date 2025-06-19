#!/usr/bin/env python3
"""
Lazada Multi-Country Scraper Orchestrator
=========================================
Scrapes both Thailand and Indonesia Lazada sites
Handles VPN checks, CAPTCHA recovery, and resume functionality
"""

# Scraping Configuration
TARGET_PRODUCTS_PER_CATEGORY = 50    # How many products to scrape per category
MAX_PAGES_TO_SCRAPE = 2              # Pages needed to get ~50 products  
DELAY_BETWEEN_PAGES_SECONDS = 2      # Wait time between pages
DELAY_BETWEEN_CATEGORIES_SECONDS = 5 # Wait time between categories
CURL_REQUEST_TIMEOUT_SECONDS = 45    # Timeout for each request

# Country Configuration
COUNTRIES = {
    "thailand": {
        "name": "Thailand",
        "flag": "🇹🇭",
        "domain": "lazada.co.th",
        "curl_file": "curl_th.txt",
        "url_template": "https://www.lazada.co.th/catalog/?ajax=true&from=hp_categories&isFirstRequest={first_request}&page={page}&q={query}&service=all_channel&src=all_channel",
        "dataset_id": "lazada_products",
        "table_id": "lazada_thailand",
        "data_dir": "data/thailand",
        "currency": "฿"
    },
    "indonesia": {
        "name": "Indonesia", 
        "flag": "🇮🇩",
        "domain": "lazada.co.id",
        "curl_file": "curl_id.txt",
        "url_template": "https://www.lazada.co.id/tag/{tag}/?ajax=true&catalog_redirect_tag=true&isFirstRequest={first_request}&page={page}&q={query}&spm=a2o4j.homepage.search.d_go",
        "dataset_id": "lazada_products", 
        "table_id": "lazada_indonesia",
        "data_dir": "data/indonesia",
        "currency": "Rp"
    },
    "malaysia": {
        "name": "Malaysia",
        "flag": "🇲🇾",
        "domain": "lazada.com.my",
        "curl_file": "curl_ml.txt",
        "url_template": "https://www.lazada.com.my/tag/{tag}/?ajax=true&catalog_redirect_tag=true&isFirstRequest={first_request}&page={page}&q={query}&spm=a2o4k.homepage.search.d_go",
        "dataset_id": "lazada_products",
        "table_id": "lazada_malaysia", 
        "data_dir": "data/malaysia",
        "currency": "RM"
    }
}

import csv
import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, date
from urllib.parse import quote_plus
import re

# BigQuery imports
try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    print("⚠️  BigQuery libraries not installed. Run: uv add google-cloud-bigquery")

# Import verify functions
try:
    from verify import check_country_categories, print_dashboard, print_next_steps, load_categories as load_categories_verify
except ImportError:
    print("⚠️  Could not import verify functions - verification will be skipped")

def show_big_red_error(error_type, country_config, details=""):
    """Show big red error message for different error types"""
    country_name = country_config.get("name", "Unknown") if country_config else "Unknown"
    curl_file = country_config.get("curl_file", "curl.txt") if country_config else "curl.txt"
    domain = country_config.get("domain", "lazada.co.th") if country_config else "lazada.co.th"
    
    print("\n" + "="*80)
    print("🚨" * 20)
    
    if error_type == "captcha":
        print("❌❌❌ WEBSITE BLOCKED YOU - ACTION NEEDED ❌❌❌")
        print("🚨" * 20)
        print("="*80)
        print()
        print(f"The {country_name} website thinks you are a robot!")
        print()
        print("WHAT TO DO RIGHT NOW:")
        print(f"   1. Open Chrome browser")
        print(f"   2. Go to https://www.{domain}/")
        print(f"   3. Search for 'hair care'")
        print(f"   4. COMPLETE THE ROBOT CHALLENGE (click pictures, etc.)")
        print(f"   5. After passing, copy new curl → Save to {curl_file}")
        print(f"   6. Run this script again")
        print()
        print("⚠️  IMPORTANT: You MUST pass the robot test first!")
        print("   The website will show puzzles - solve them completely!")
        print("   Only then copy the curl command!")
        
    elif error_type == "vpn":
        current_country = details if details else "Unknown"
        print("❌❌❌ VPN REQUIRED FOR WORKFLOW ❌❌❌")
        print("🚨" * 20)
        print("="*80)
        print()
        print(f"Your location is detected as: {current_country}")
        print("You are currently in India")
        print()
        print("WHAT TO DO RIGHT NOW:")
        print("   1. Connect to VPN (any non-India country)")
        print("   2. Wait 30 seconds for IP to change")
        print("   3. Run this script again")
        print()
        print("⚠️  WHY VPN IS REQUIRED:")
        print("   • Indonesia website is blocked from India")
        print("   • Thailand works but VPN ensures consistent workflow")
        print("   • Prevents potential IP bans from heavy scraping")
        print("   • Required security practice for this operation")
        
    elif error_type == "bigquery":
        print("❌❌❌ DATABASE ERROR ❌❌❌")
        print("🚨" * 20)
        print("="*80)
        print()
        print("Cannot connect to BigQuery database")
        print()
        print("WHAT TO DO RIGHT NOW:")
        print("   Contact nilesh@pollen.tech with this error:")
        print(f"   {details}")
        
    elif error_type == "curl_missing":
        print(f"❌❌❌ {curl_file.upper()} FILE MISSING ❌❌❌")
        print("🚨" * 20)
        print("="*80)
        print()
        print(f"The {curl_file} file is required for {country_name} scraping")
        print()
        print("WHAT TO DO RIGHT NOW:")
        print(f"   1. Open Chrome → Go to https://www.{domain}/")
        print(f"   2. Search for any product")
        print(f"   3. Copy curl command from Network tab")
        print(f"   4. Save it to {curl_file}")
        print(f"   5. Run this script again")
    
    print()
    print("="*80)
    print("🚨" * 20)
    print("="*80)

def check_vpn():
    """Check if VPN is working (not in India)"""
    print("🔍 Checking VPN status...")
    
    CHECK_URL = "https://ipinfo.io/json"
    TIMEOUT = 5
    
    try:
        with urllib.request.urlopen(CHECK_URL, timeout=TIMEOUT) as resp:
            data = json.load(resp)
            country = data.get("country", "Unknown")
            print(f"   🌍 Current country: {country}")
            
            if country == "IN":
                show_big_red_error("vpn", None, country)
                return False, country
            else:
                print("✅ VPN OK - Not in India")
                return True, country
                
    except Exception as e:
        print(f"⚠️  Couldn't check IP: {e}")
        return True, "Unknown"  # Assume OK if can't check

def load_categories():
    """Load categories from categories.json"""
    try:
        with open('categories.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ categories.json not found!")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in categories.json: {e}")
        sys.exit(1)

def load_curl_command(country_config):
    """Load curl command for specific country"""
    curl_file = country_config["curl_file"]
    try:
        with open(curl_file, 'r', encoding='utf-8') as f:
            return f.read().strip()
    except FileNotFoundError:
        show_big_red_error("curl_missing", country_config)
        return None

def validate_curl_command(curl_template, country_config):
    """Test if the curl command is valid"""
    country_name = country_config["name"]
    print(f"🔍 Validating {country_name} curl command...")
    
    # Build test URL
    if country_config["name"] == "Thailand":
        test_url = country_config["url_template"].format(
            first_request="true", page=1, query=quote_plus("test")
        )
    else:  # Indonesia
        test_url = country_config["url_template"].format(
            tag="test", first_request="true", page=1, query=quote_plus("test")
        )
    
    # Replace URL in curl command
    test_curl = re.sub(r"curl '[^']*'", f"curl '{test_url}'", curl_template)
    
    try:
        result = subprocess.run(test_curl, shell=True, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            print(f"❌ Curl command failed")
            return False
            
        if not result.stdout.strip():
            print("❌ Empty response")
            return False
            
        # Check if we got HTML (blocked) instead of JSON
        if result.stdout.strip().startswith('<'):
            print("❌ Got HTML instead of JSON - likely blocked")
            return False
            
        # Try to parse as JSON and check for CAPTCHA
        try:
            data = json.loads(result.stdout)
            if "ret" in data and isinstance(data["ret"], list):
                if "FAIL_SYS_USER_VALIDATE" in str(data["ret"]):
                    print("❌ CAPTCHA challenge detected")
                    return False
            print(f"✅ {country_name} curl command is valid!")
            return True
        except json.JSONDecodeError:
            print("❌ Response is not valid JSON")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Curl command timed out")
        return False
    except Exception as e:
        print(f"❌ Error validating curl: {e}")
        return False

def init_bigquery():
    """Initialize BigQuery client using key.json"""
    if not BIGQUERY_AVAILABLE:
        return None
        
    try:
        if os.path.exists('key.json'):
            credentials = service_account.Credentials.from_service_account_file('key.json')
            client = bigquery.Client(credentials=credentials)
        else:
            show_big_red_error("bigquery", {}, "key.json not found - email nilesh@pollen.tech")
            return None
        
        print("✅ BigQuery client initialized")
        return client
    except Exception as e:
        show_big_red_error("bigquery", {}, str(e))
        return None

def check_today_run(client, country_config):
    """Check if scraping was already done today for this country"""
    if not client:
        return False
        
    try:
        today = date.today().strftime('%Y-%m-%d')
        dataset_id = country_config["dataset_id"]
        table_id = country_config["table_id"]
        
        query = f"""
        SELECT COUNT(*) as count
        FROM `{client.project}.{dataset_id}.{table_id}`
        WHERE DATE(scraped_at) = '{today}'
        """
        
        results = client.query(query)
        for row in results:
            if row.count > 0:
                print(f"⚠️  Found {row.count} records already scraped today for {country_config['name']} ({today})")
                return True
        
        print(f"✅ No records found for {country_config['name']} today ({today})")
        return False
        
    except Exception as e:
        print(f"❌ Error checking today's run for {country_config['name']}: {e}")
        return False

def upload_to_bigquery(client, country_config, products, category_name):
    """Upload products to BigQuery"""
    if not client or not products:
        return False
        
    try:
        # Add metadata to each product
        scraped_at = datetime.utcnow().isoformat() + 'Z'
        for product in products:
            product['category_name'] = category_name
            product['scraped_at'] = scraped_at
        
        dataset_id = country_config["dataset_id"]
        table_id = country_config["table_id"]
        table_ref = client.dataset(dataset_id).table(table_id)
        
        # Insert data
        errors = client.insert_rows_json(table_ref, products)
        
        if errors:
            print(f"❌ BigQuery insert errors: {errors}")
            return False
        else:
            print(f"✅ Uploaded {len(products)} products to BigQuery")
            return True
            
    except Exception as e:
        print(f"❌ Error uploading to BigQuery: {e}")
        return False

def run_curl_for_query(curl_template, country_config, query, page=1):
    """Execute curl command with injected query and page"""
    encoded_query = quote_plus(query)
    
    # Build the new URL based on country
    if country_config["name"] == "Thailand":
        new_url = country_config["url_template"].format(
            first_request=str(page==1).lower(), page=page, query=encoded_query
        )
    else:  # Indonesia
        tag_name = query.lower().replace(' ', '-').replace('&', 'and').replace(',', '')
        new_url = country_config["url_template"].format(
            tag=tag_name, first_request=str(page==1).lower(), page=page, query=encoded_query
        )
    
    # Replace URL in curl command
    curl_cmd = re.sub(r"curl '[^']*'", f"curl '{new_url}'", curl_template)
    
    try:
        print(f"  🌐 Fetching page {page}...")
        result = subprocess.run(curl_cmd, shell=True, capture_output=True, text=True, timeout=CURL_REQUEST_TIMEOUT_SECONDS)
        
        if result.returncode != 0:
            print(f"  ❌ Curl failed: {result.stderr}")
            return None
            
        if not result.stdout.strip():
            print(f"  ⚠️  Empty response")
            return None
            
        # Check if we got HTML (blocked) instead of JSON
        if result.stdout.strip().startswith('<'):
            print(f"  ❌ Got HTML instead of JSON (likely blocked)")
            return None
            
        data = json.loads(result.stdout)
        return data
        
    except subprocess.TimeoutExpired:
        print(f"  ❌ Request timeout")
        return None
    except json.JSONDecodeError as e:
        print(f"  ❌ JSON parse error: {e}")
        return None
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None

def parse_products(data, country_config):
    """Extract product data from API response"""
    if not data:
        return []
    
    items = data.get("mods", {}).get("listItems", [])
    products = []
    
    for item in items:
        # Calculate discount percentage
        discount_percent = ""
        current_price = item.get("price", "")
        original_price = item.get("originalPrice", "")
        
        if current_price and original_price:
            try:
                current = float(current_price)
                original = float(original_price)
                if original > current:
                    discount_percent = f"{((original - current) / original * 100):.1f}%"
            except (ValueError, TypeError):
                pass
        
        # Format price with currency
        currency = country_config["currency"]
        formatted_original = f"{currency}{original_price}" if original_price else ""
        
        product = {
            'name': item.get('name', ''),
            'current_price': item.get('priceShow', ''),
            'original_price': formatted_original,
            'discount_percent': discount_percent,
            'rating': item.get('ratingScore', ''),
            'reviews': item.get('review', ''),
            'location': item.get('location', ''),
            'item_id': item.get('itemId', ''),
            'seller_name': item.get('sellerName', ''),
            'brand_name': item.get('brandName', ''),
            'image_url': item.get('image', ''),
        }
        products.append(product)
    
    return products

def save_to_csv(products, category_name, country_config, timestamp_dir):
    """Save products to CSV file in timestamped directory"""
    # Create timestamped subdirectory
    data_dir = f"{country_config['data_dir']}/{timestamp_dir}"
    os.makedirs(data_dir, exist_ok=True)
    
    # Clean filename
    filename = f"{data_dir}/{category_name.replace(' ', '_').replace('&', 'and').replace(',', '').lower()}.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        if not products:
            # Create empty file with headers
            writer = csv.writer(f)
            writer.writerow(['name', 'current_price', 'original_price', 'discount_percent', 
                           'rating', 'reviews', 'location', 'item_id', 'seller_name', 
                           'brand_name', 'image_url'])
            return filename
        
        writer = csv.DictWriter(f, fieldnames=products[0].keys())
        writer.writeheader()
        writer.writerows(products)
    
    return filename

def scrape_category(curl_template, category_name, query, country_config, timestamp_dir, bq_client):
    """Scrape a single category"""
    print(f"🔍 {category_name} → '{query}'")
    
    all_products = []
    
    for page in range(1, MAX_PAGES_TO_SCRAPE + 1):
        data = run_curl_for_query(curl_template, country_config, query, page)
        if not data:
            break
            
        # Check for CAPTCHA specifically
        if "ret" in data and isinstance(data["ret"], list):
            if "FAIL_SYS_USER_VALIDATE" in str(data["ret"]):
                print(f"  🚨 CAPTCHA detected on page {page}")
                return "CAPTCHA", len(all_products)
        
        products = parse_products(data, country_config)
        if not products:
            print(f"  ⚠️  No products found on page {page}, stopping")
            break
            
        all_products.extend(products)
        print(f"  ✅ Page {page}: {len(products)} products")
        
        # Stop if we have enough products
        if len(all_products) >= TARGET_PRODUCTS_PER_CATEGORY:
            print(f"  🎯 Target reached: {len(all_products)} products")
            break
        
        # Delay between pages
        if page < MAX_PAGES_TO_SCRAPE:
            print(f"  ⏳ Waiting {DELAY_BETWEEN_PAGES_SECONDS} seconds before next page...")
            time.sleep(DELAY_BETWEEN_PAGES_SECONDS)
    
    # Save to CSV
    csv_filename = save_to_csv(all_products, category_name, country_config, timestamp_dir)
    print(f"  💾 Saved {len(all_products)} products to {csv_filename}")
    
    # Upload to BigQuery
    if bq_client and all_products:
        upload_to_bigquery(bq_client, country_config, all_products, category_name)
    
    return "SUCCESS", len(all_products)

def scrape_country(country_key, categories, bq_client):
    """Scrape a single country completely"""
    country_config = COUNTRIES[country_key]
    country_name = country_config["name"]
    flag = country_config["flag"]
    
    print(f"\n{flag} Starting {country_name} scraping...")
    print("=" * 60)
    
    # Load and validate curl
    curl_template = load_curl_command(country_config)
    if not curl_template:
        return False, 0, 0
    
    if not validate_curl_command(curl_template, country_config):
        show_big_red_error("captcha", country_config)
        return False, 0, 0
    
    # Check if already scraped today in BigQuery (PRIMARY CHECK)
    if bq_client and check_today_run(bq_client, country_config):
        response = input(f"\n❓ {country_name} already scraped today in BigQuery. Continue anyway? (y/N): ")
        if response.lower() not in ['y', 'yes']:
            print(f"🛑 {country_name} scraping cancelled - already completed today")
            return True, 0, 0  # Skip this country but continue overall
    
    # Set up timestamp for this run
    timestamp_dir = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    print(f"📁 {country_name} CSV files will be saved to: {country_config['data_dir']}/{timestamp_dir}/")
    
    # Filter categories (scrape all since BigQuery is source of truth)
    remaining_categories = categories
    
    print(f"📋 Total categories: {len(categories)}")
    print(f"📋 Categories to scrape: {len(remaining_categories)}")
    
    # Confirmation
    print(f"\n📊 Ready to scrape {len(remaining_categories)} {country_name} categories")
    print(f"🎯 Target: {TARGET_PRODUCTS_PER_CATEGORY} products per category")
    
    response = input(f"\n❓ Start {country_name} scraping? (Y/n): ")
    if response.lower() in ['n', 'no']:
        print(f"🛑 {country_name} scraping cancelled by user")
        return True, 0, 0  # Skip but don't fail
    
    # Scrape each remaining category
    total_products = 0
    successful_categories = 0
    
    for i, (category_name, query) in enumerate(remaining_categories.items()):
        try:
            status, count = scrape_category(curl_template, category_name, query, country_config, timestamp_dir, bq_client)
            total_products += count
            
            if status == "SUCCESS" and count > 0:
                successful_categories += 1
            elif status == "CAPTCHA":
                print(f"  🚨 CAPTCHA detected for {category_name}")
                show_big_red_error("captcha", country_config)
                print(f"\n📊 {country_name} progress so far:")
                print(f"   ✅ {successful_categories} categories completed")
                print(f"   📦 {total_products} products scraped")
                print(f"   📁 Data saved to: {country_config['data_dir']}/{timestamp_dir}/")
                print(f"\n💡 Fix the curl and run script again to resume {country_name}!")
                return False, successful_categories, total_products
                
            # Delay between categories
            if i < len(remaining_categories) - 1:
                print(f"⏳ Waiting {DELAY_BETWEEN_CATEGORIES_SECONDS} seconds before next category...")
                time.sleep(DELAY_BETWEEN_CATEGORIES_SECONDS)
                
        except KeyboardInterrupt:
            print(f"\n⚠️  {country_name} interrupted by user")
            return False, successful_categories, total_products
        except Exception as e:
            print(f"  ❌ Error scraping {category_name}: {e}")
    
    # Summary
    print(f"\n{flag} {country_name} scraping complete!")
    print(f"   📊 {successful_categories}/{len(remaining_categories)} categories successful")
    print(f"   📦 {total_products} total products scraped")
    print(f"   📁 CSV files saved to: {country_config['data_dir']}/{timestamp_dir}/")
    if bq_client:
        print(f"   🗄️  Data uploaded to BigQuery: {country_config['dataset_id']}.{country_config['table_id']}")
    
    return True, successful_categories, total_products

def main():
    print("🌏 Lazada Multi-Country Scraper")
    print("=" * 50)
    print(f"🎯 Target: {TARGET_PRODUCTS_PER_CATEGORY} products per category")
    print("🇹🇭 Thailand → 🇮🇩 Indonesia → 🇲🇾 Malaysia")
    
    # Pre-checks
    print("\n🔧 Pre-flight checks...")
    
    # 1. VPN Check
    vpn_ok, current_country = check_vpn()
    if not vpn_ok:
        sys.exit(1)
    
    # 2. Load categories
    categories = load_categories()
    print(f"✅ Loaded {len(categories)} categories")
    
    # 3. BigQuery
    bq_client = init_bigquery()
    if not bq_client:
        sys.exit(1)
    
    print("✅ All pre-checks passed!")
    
    # Start scraping countries sequentially
    overall_start_time = datetime.now()
    
    # Thailand first
    print("\n" + "="*70)
    print("🇹🇭 THAILAND SCRAPING")
    print("="*70)
    
    th_success, th_categories, th_products = scrape_country("thailand", categories, bq_client)
    
    if not th_success:
        print("\n❌ Thailand scraping failed - fix the issue and run script again")
        sys.exit(1)
    
    # Indonesia second
    print("\n" + "="*70)
    print("🇮🇩 INDONESIA SCRAPING")
    print("="*70)
    
    id_success, id_categories, id_products = scrape_country("indonesia", categories, bq_client)
    
    if not id_success:
        print("\n❌ Indonesia scraping failed - fix the issue and run script again")
        sys.exit(1)
    
    # Malaysia third
    print("\n" + "="*70)
    print("🇲🇾 MALAYSIA SCRAPING")
    print("="*70)
    
    ml_success, ml_categories, ml_products = scrape_country("malaysia", categories, bq_client)
    
    if not ml_success:
        print("\n❌ Malaysia scraping failed - fix the issue and run script again")
        sys.exit(1)
    
    # Final summary
    total_time = datetime.now() - overall_start_time
    print("\n" + "="*70)
    print("🎉 MULTI-COUNTRY SCRAPING COMPLETE!")
    print("="*70)
    print(f"🇹🇭 Thailand: {th_categories} categories, {th_products} products")
    print(f"🇮🇩 Indonesia: {id_categories} categories, {id_products} products")
    print(f"🇲🇾 Malaysia: {ml_categories} categories, {ml_products} products")
    print(f"📦 Total: {th_products + id_products + ml_products} products")
    print(f"⏰ Total time: {total_time}")
    print(f"🗄️  All data uploaded to BigQuery")
    print(f"💾 CSV backups saved to data/ directory")
    print(f"🕐 Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run verification dashboard
    print("\n" + "="*70)
    print("🔍 VERIFICATION CHECK")
    print("="*70)
    
    try:
        # Check all countries with verify script
        expected_categories = load_categories_verify()
        if expected_categories and bq_client:
            country_results = {}
            for country_key in ["thailand", "indonesia", "malaysia"]:
                country_results[country_key] = check_country_categories(
                    bq_client, country_key, expected_categories, verbose=False
                )
            
            # Print compact dashboard
            issues_found, total_products, total_target = print_dashboard(
                country_results, expected_categories, verbose=False
            )
            
            # Print next steps if issues
            print_next_steps(country_results, verbose=False)
            
        else:
            print("⚠️  Verification skipped - could not load categories or BigQuery")
    
    except Exception as e:
        print(f"⚠️  Verification check failed: {e}")
        print("💡 Run 'python verify.py' manually to check results")

if __name__ == "__main__":
    main() 