#!/usr/bin/env python3
"""
Lazada Multi-Country Verification Script
========================================
Intern-friendly dashboard showing today's scraping status and issues
"""

import json
import os
import sys
import argparse
from datetime import datetime, date

# BigQuery imports
try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    print("⚠️  BigQuery libraries not installed. Run: uv add google-cloud-bigquery")

# Constants
TARGET_PRODUCTS_PER_CATEGORY = 50
MIN_PRODUCTS_PER_CATEGORY = 30
COUNTRIES = {
    "thailand": {"name": "Thailand", "flag": "🇹🇭", "table": "lazada_thailand"},
    "indonesia": {"name": "Indonesia", "flag": "🇮🇩", "table": "lazada_indonesia"}, 
    "malaysia": {"name": "Malaysia", "flag": "🇲🇾", "table": "lazada_malaysia"}
}

def init_bigquery():
    """Initialize BigQuery client"""
    if not BIGQUERY_AVAILABLE:
        return None
        
    try:
        if os.path.exists('key.json'):
            credentials = service_account.Credentials.from_service_account_file('key.json')
            return bigquery.Client(credentials=credentials)
        else:
            print("❌ key.json not found!")
            return None
    except Exception as e:
        print(f"❌ Failed to initialize BigQuery: {e}")
        return None

def load_categories():
    """Load expected categories"""
    try:
        with open('categories.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Cannot load categories.json: {e}")
        return {}

def check_country_categories(client, country_key, expected_categories, verbose=False):
    """Check categories for a country, return issues"""
    country = COUNTRIES[country_key]
    today = date.today().strftime('%Y-%m-%d')
    
    try:
        query = f"""
        SELECT 
            category_name,
            COUNT(*) as product_count
        FROM `{client.project}.lazada_products.{country['table']}`
        WHERE DATE(scraped_at) = '{today}'
        GROUP BY category_name
        ORDER BY category_name
        """
        
        results = client.query(query)
        scraped_data = {row.category_name: row.product_count for row in results}
        
        # Find issues
        missing_categories = []
        low_count_categories = []
        
        for category_name in expected_categories.keys():
            if category_name not in scraped_data:
                missing_categories.append(category_name)
            elif scraped_data[category_name] < MIN_PRODUCTS_PER_CATEGORY:
                low_count_categories.append((category_name, scraped_data[category_name]))
        
        total_products = sum(scraped_data.values())
        categories_done = len(scraped_data)
        
        return {
            'country_key': country_key,
            'total_products': total_products,
            'categories_done': categories_done,
            'missing_categories': missing_categories,
            'low_count_categories': low_count_categories,
            'scraped_data': scraped_data if verbose else None
        }
        
    except Exception as e:
        if verbose:
            print(f"❌ Error checking {country['name']}: {e}")
        return {
            'country_key': country_key,
            'total_products': 0,
            'categories_done': 0,
            'missing_categories': list(expected_categories.keys()),
            'low_count_categories': [],
            'error': str(e)
        }

def get_status_emoji(categories_done, total_products, missing_cats, low_cats, expected_total):
    """Get status emoji and color"""
    if total_products == 0:
        return "❌", "FAIL"
    elif missing_cats or low_cats:
        return "⚠️", "WARN"
    elif categories_done == expected_total and total_products >= expected_total * TARGET_PRODUCTS_PER_CATEGORY * 0.8:
        return "✅", "OK"  
    elif total_products > 0:
        return "✅", "OK"
    else:
        return "❌", "FAIL"

def print_dashboard(country_results, expected_categories, verbose=False):
    """Print compact dashboard"""
    today = date.today().strftime('%Y-%m-%d')
    total_target = len(expected_categories) * TARGET_PRODUCTS_PER_CATEGORY * 3
    
    print("=" * 60)
    print("🔍 LAZADA DAILY CHECK")
    print("=" * 60)
    print(f"📅 Date: {today}   🎯 Target: {total_target} products")
    print()
    
    # Header
    print("COUNTRY      CATS   PRODUCTS   STATUS   ISSUES")
    print("-" * 60)
    
    # Country rows
    total_products = 0
    total_categories = 0
    issues_found = False
    
    for country_key, result in country_results.items():
        country = COUNTRIES[country_key]
        emoji, status = get_status_emoji(
            result['categories_done'], 
            result['total_products'],
            result['missing_categories'],
            result['low_count_categories'], 
            len(expected_categories)
        )
        
        # Count issues
        issue_count = len(result['missing_categories']) + len(result['low_count_categories'])
        issues_text = f"{issue_count} issues" if issue_count > 0 else "–"
        
        if issue_count > 0:
            issues_found = True
            
        print(f"{country['flag']} {country['name']:<9} {result['categories_done']:>2}/22   {result['total_products']:>4}       {emoji}     {issues_text}")
        
        total_products += result['total_products']
        total_categories += result['categories_done']
    
    # Summary row
    print("-" * 60)
    progress_pct = int((total_products / total_target) * 100) if total_target > 0 else 0
    overall_emoji = "⚠️" if issues_found else "✅" if progress_pct >= 80 else "⚠️"
    
    print(f"TOTAL        {total_categories:>2}/66   {total_products:>4}       {overall_emoji}     {progress_pct}%")
    print("=" * 60)
    
    return issues_found, total_products, total_target

def print_next_steps(country_results, verbose=False):
    """Print actionable next steps"""
    issues = []
    
    for country_key, result in country_results.items():
        country = COUNTRIES[country_key]
        
        if result['missing_categories']:
            issues.append(f"{country['flag']} {country['name']}: {len(result['missing_categories'])} missing categories")
            if verbose:
                issues.append(f"   Missing: {', '.join(result['missing_categories'][:5])}")
        
        if result['low_count_categories']:
            issues.append(f"{country['flag']} {country['name']}: {len(result['low_count_categories'])} categories < {MIN_PRODUCTS_PER_CATEGORY} products")
            if verbose:
                low_cats_text = [f"{cat} ({count})" for cat, count in result['low_count_categories'][:3]]
                issues.append(f"   Low counts: {', '.join(low_cats_text)}")
    
    if not issues:
        print("🎉 All good – nothing to do!")
        return True
    else:
        print("📋 ISSUES FOUND:")
        for issue in issues:
            print(f"   {issue}")
        print("\n💡 Next step: Get fresh curl files and re-run scraper for affected countries")
        return False

def print_verbose_details(country_results, client):
    """Print detailed information in verbose mode"""
    print("\n" + "=" * 60)
    print("📊 DETAILED BREAKDOWN (--verbose)")
    print("=" * 60)
    
    for country_key, result in country_results.items():
        country = COUNTRIES[country_key]
        print(f"\n{country['flag']} {country['name']} Details:")
        
        if 'error' in result:
            print(f"   ❌ Error: {result['error']}")
            continue
            
        if result['scraped_data']:
            # Show all categories with counts
            print(f"   📊 All categories today:")
            for cat_name, count in sorted(result['scraped_data'].items()):
                status = "✅" if count >= MIN_PRODUCTS_PER_CATEGORY else "⚠️"
                print(f"      {status} {cat_name}: {count} products")
        
        # Historical data
        if client:
            try:
                query = f"""
                SELECT 
                    COUNT(DISTINCT DATE(scraped_at)) as days,
                    COUNT(*) as total_products
                FROM `{client.project}.lazada_products.{country['table']}`
                """
                results = client.query(query)
                for row in results:
                    print(f"   📈 Historical: {row.total_products} products over {row.days} days")
            except:
                pass

def main():
    parser = argparse.ArgumentParser(description='Check Lazada scraping status')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Show detailed breakdown and historical data')
    args = parser.parse_args()
    
    # Initialize
    client = init_bigquery()
    if not client:
        print("❌ Cannot connect to BigQuery")
        sys.exit(1)
    
    expected_categories = load_categories()
    if not expected_categories:
        sys.exit(1)
    
    # Check all countries
    country_results = {}
    for country_key in COUNTRIES.keys():
        country_results[country_key] = check_country_categories(
            client, country_key, expected_categories, args.verbose
        )
    
    # Print dashboard
    issues_found, total_products, total_target = print_dashboard(
        country_results, expected_categories, args.verbose
    )
    
    # Print next steps
    all_good = print_next_steps(country_results, args.verbose)
    
    # Verbose details
    if args.verbose:
        print_verbose_details(country_results, client)
    
    # Exit code for automation
    if all_good and total_products >= total_target * 0.8:
        sys.exit(0)  # Success
    else:
        sys.exit(1)  # Issues found

if __name__ == "__main__":
    main() 