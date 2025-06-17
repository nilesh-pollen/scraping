#!/usr/bin/env python3
"""
Lazada Thailand Scraper with BigQuery Integration
================================================
Reads categories from categories.json, uses curl.txt for authentication,
and saves product data to BigQuery with duplicate checking.

Usage:
    python scrape.py

Update curl.txt with fresh browser cookies/headers before running.
"""

import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, date
from urllib.parse import quote_plus

# BigQuery imports
try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    print("⚠️  BigQuery libraries not installed. Run: uv add google-cloud-bigquery")


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


def load_curl_command():
    """Load curl command from curl.txt"""
    try:
        with open('curl.txt', 'r', encoding='utf-8') as f:
            return f.read().strip()
    except FileNotFoundError:
        print("❌ curl.txt not found!")
        print("   Please read README.md for instructions on how to get a fresh curl command.")
        sys.exit(1)


def validate_curl_command(curl_template):
    """Test if the curl command is valid by making a simple request"""
    print("🔍 Validating curl command...")
    
    # Use a simple search query to test
    test_url = "https://www.lazada.co.th/catalog/?ajax=true&from=hp_categories&isFirstRequest=true&page=1&q=test&service=all_channel&src=all_channel"
    
    import re
    test_curl = re.sub(r"curl '[^']*'", f"curl '{test_url}'", curl_template)
    
    try:
        result = subprocess.run(test_curl, shell=True, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            print(f"❌ Curl command failed: {result.stderr}")
            return False
            
        if not result.stdout.strip():
            print("❌ Empty response from curl command")
            return False
            
        # Check if we got HTML (blocked) instead of JSON
        if result.stdout.strip().startswith('<'):
            print("❌ Got HTML instead of JSON - cookies likely expired")
            return False
            
        # Try to parse as JSON
        try:
            data = json.loads(result.stdout)
            if "mods" in data:
                print("✅ Curl command is valid!")
                return True
            else:
                print("❌ Unexpected JSON structure")
                return False
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
        # Use key.json for authentication
        if os.path.exists('key.json'):
            credentials = service_account.Credentials.from_service_account_file('key.json')
            client = bigquery.Client(credentials=credentials)
        else:
            print("❌ key.json not found!")
            print("   Please make sure key.json is in the current directory")
            return None
        
        print("✅ BigQuery client initialized")
        return client
    except Exception as e:
        print(f"❌ Failed to initialize BigQuery: {e}")
        print("   Make sure key.json has the correct permissions")
        return None


def check_today_run(client, dataset_id, table_id):
    """Check if scraping was already done today"""
    if not client:
        return False
        
    try:
        today = date.today().strftime('%Y-%m-%d')
        query = f"""
        SELECT COUNT(*) as count
        FROM `{client.project}.{dataset_id}.{table_id}`
        WHERE DATE(scraped_at) = '{today}'
        """
        
        results = client.query(query)
        for row in results:
            if row.count > 0:
                print(f"⚠️  Found {row.count} records already scraped today ({today})")
                return True
        
        print(f"✅ No records found for today ({today})")
        return False
        
    except Exception as e:
        print(f"❌ Error checking today's run: {e}")
        return False


def upload_to_bigquery(client, dataset_id, table_id, products, category_name):
    """Upload products to BigQuery"""
    if not client or not products:
        return False
        
    try:
        # Add metadata to each product
        scraped_at = datetime.utcnow().isoformat() + 'Z'  # Convert to ISO string format
        for product in products:
            product['category_name'] = category_name
            product['scraped_at'] = scraped_at
        
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


def run_curl_for_query(curl_template, query, page=1):
    """Execute curl command with injected query and page"""
    encoded_query = quote_plus(query)
    
    # Build the new URL with our query and page
    new_url = (
        f"https://www.lazada.co.th/catalog/?ajax=true&from=hp_categories"
        f"&isFirstRequest={str(page==1).lower()}&page={page}"
        f"&q={encoded_query}&service=all_channel&src=all_channel"
    )
    
    # Simple approach: just replace the URL in the curl command
    import re
    curl_cmd = re.sub(
        r"curl '[^']*'", 
        f"curl '{new_url}'", 
        curl_template
    )
    
    try:
        print(f"  🌐 Fetching page {page}...")
        result = subprocess.run(curl_cmd, shell=True, capture_output=True, text=True, timeout=45)
        
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
        print(f"  Response preview: {result.stdout[:200]}...")
        return None
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None


def parse_products(data):
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
        
        product = {
            'name': item.get('name', ''),
            'current_price': item.get('priceShow', ''),
            'original_price': f"฿{original_price}" if original_price else "",
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


def save_to_csv(products, category_name):
    """Save products to CSV file in data/ directory (backup)"""
    os.makedirs('data', exist_ok=True)
    
    # Clean filename
    filename = f"data/{category_name.replace(' ', '_').replace('&', 'and').replace(',', '').lower()}.csv"
    
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


def scrape_category(curl_template, category_name, query, bq_client, dataset_id, table_id, max_pages=5):
    """Scrape a single category with pagination"""
    print(f"🔍 {category_name} → '{query}'")
    
    all_products = []
    
    for page in range(1, max_pages + 1):
        data = run_curl_for_query(curl_template, query, page)
        if not data:
            break
            
        products = parse_products(data)
        if not products:
            print(f"  ⚠️  No products found on page {page}, stopping")
            break
            
        all_products.extend(products)
        print(f"  ✅ Page {page}: {len(products)} products")
        
        # Longer delay between pages to avoid rate limiting
        if page < max_pages:  # Don't sleep after the last page
            print(f"  ⏳ Waiting 2 seconds before next page...")
            time.sleep(2)
    
    # Save to CSV (backup)
    csv_filename = save_to_csv(all_products, category_name)
    print(f"  💾 Saved {len(all_products)} products to {csv_filename}")
    
    # Upload to BigQuery
    if bq_client and all_products:
        upload_to_bigquery(bq_client, dataset_id, table_id, all_products, category_name)
    
    return len(all_products)


def main():
    print("🚀 Lazada Thailand Scraper with BigQuery")
    print("=" * 50)
    
    # Configuration - Updated to use your actual setup
    DATASET_ID = "lazada_products"
    TABLE_ID = "lazada_thailand"
    
    # Load configuration
    categories = load_categories()
    curl_template = load_curl_command()
    
    print(f"📋 Found {len(categories)} categories to scrape")
    
    # Step 1: Validate curl command
    if not validate_curl_command(curl_template):
        print("\n❌ Curl validation failed!")
        print("📖 Please read README.md for instructions on how to get a fresh curl command.")
        sys.exit(1)
    
    # Step 2: Initialize BigQuery
    bq_client = init_bigquery()
    
    if bq_client:
        # Step 3: Check if already run today
        if check_today_run(bq_client, DATASET_ID, TABLE_ID):
            response = input("\n❓ Data already scraped today. Continue anyway? (y/N): ")
            if response.lower() not in ['y', 'yes']:
                print("🛑 Scraping cancelled by user")
                sys.exit(0)
    
    # Step 4: Final confirmation
    print(f"\n📊 Ready to scrape {len(categories)} categories")
    print(f"💾 Data will be saved to: CSV files + BigQuery ({DATASET_ID}.{TABLE_ID})" if bq_client else "💾 Data will be saved to: CSV files only")
    
    response = input("\n❓ Start scraping? (Y/n): ")
    if response.lower() in ['n', 'no']:
        print("🛑 Scraping cancelled by user")
        sys.exit(0)
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # Step 5: Scrape each category
    total_products = 0
    successful_categories = 0
    
    for i, (category_name, query) in enumerate(categories.items()):
        try:
            count = scrape_category(curl_template, category_name, query, bq_client, DATASET_ID, TABLE_ID)
            total_products += count
            if count > 0:
                successful_categories += 1
                
            # Longer delay between categories to be respectful
            if i < len(categories) - 1:  # Don't sleep after the last category
                print(f"⏳ Waiting 5 seconds before next category...")
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("\n⚠️  Interrupted by user")
            break
        except Exception as e:
            print(f"  ❌ Error scraping {category_name}: {e}")
    
    # Summary
    print("\n" + "=" * 50)
    print(f"✅ Scraping complete!")
    print(f"   📊 {successful_categories}/{len(categories)} categories successful")
    print(f"   📦 {total_products} total products scraped")
    print(f"   📁 Data saved to data/ directory")
    if bq_client:
        print(f"   🗄️  Data uploaded to BigQuery: {DATASET_ID}.{TABLE_ID}")
    print(f"   🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main() 