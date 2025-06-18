#!/usr/bin/env python3
"""
Quick Data Check - Lazada Thailand Scraper
==========================================
Check if scraping ran today and show stats
"""

import os
from datetime import date
from google.cloud import bigquery
from google.oauth2 import service_account

def get_client():
    """Get BigQuery client using key.json"""
    if not os.path.exists('key.json'):
        print("❌ key.json not found!")
        print("   Email nilesh@pollen.tech to get the file")
        return None
    
    try:
        credentials = service_account.Credentials.from_service_account_file('key.json')
        client = bigquery.Client(credentials=credentials)
        return client
    except Exception as e:
        print(f"❌ BigQuery connection failed: {e}")
        return None

def check_today_and_stats(client):
    """Check if ran today and show stats"""
    dataset_id = "lazada_products"
    table_id = "lazada_thailand"
    today = date.today().strftime('%Y-%m-%d')
    
    print(f"🔍 Checking data for {today}...")
    
    # Check if ran today
    today_query = f"""
    SELECT 
        category_name,
        COUNT(*) as products,
        MAX(scraped_at) as scraped_time
    FROM `{client.project}.{dataset_id}.{table_id}`
    WHERE DATE(scraped_at) = '{today}'
    GROUP BY category_name
    ORDER BY category_name
    """
    
    # All-time stats
    alltime_query = f"""
    SELECT 
        COUNT(*) as total_products,
        COUNT(DISTINCT category_name) as total_categories,
        COUNT(DISTINCT DATE(scraped_at)) as total_days,
        MIN(DATE(scraped_at)) as first_run,
        MAX(DATE(scraped_at)) as last_run
    FROM `{client.project}.{dataset_id}.{table_id}`
    """
    
    try:
        # Today's data
        today_results = list(client.query(today_query))
        
        if today_results:
            print(f"✅ Scraping ran today! ({today})")
            print(f"\n📊 Today's Results:")
            
            total_today = 0
            for row in today_results:
                print(f"   • {row.category_name}: {row.products:,} products")
                total_today += row.products
            
            print(f"\n🎯 Today's Total: {total_today:,} products across {len(today_results)} categories")
            
            # Show scrape time
            if today_results:
                scrape_time = today_results[0].scraped_time.strftime('%H:%M:%S')
                print(f"⏰ Scraped at: {scrape_time}")
        else:
            print(f"❌ No data found for today ({today})")
        
        # All-time stats
        print(f"\n📈 All-Time Stats:")
        alltime_results = list(client.query(alltime_query))
        
        if alltime_results:
            row = alltime_results[0]
            print(f"   📦 Total products: {row.total_products:,}")
            print(f"   📋 Total categories: {row.total_categories}")
            print(f"   📅 Total run days: {row.total_days}")
            print(f"   🗓️  First run: {row.first_run}")
            print(f"   🗓️  Last run: {row.last_run}")
            
            if row.total_days > 0:
                avg_per_day = row.total_products / row.total_days
                print(f"   📊 Average per day: {avg_per_day:,.0f} products")
        
        return len(today_results) > 0
        
    except Exception as e:
        print(f"❌ Query error: {e}")
        return False

def main():
    print("🔍 Lazada Thailand Data Check")
    print("=" * 40)
    
    # Get client
    client = get_client()
    if not client:
        return
    
    # Check today and show stats
    ran_today = check_today_and_stats(client)
    
    print(f"\n{'✅' if ran_today else '❌'} Status: {'Ran today' if ran_today else 'Not run today'}")
    
    if not ran_today:
        print("\n💡 To run scraper: python scrape.py")

if __name__ == "__main__":
    main() 