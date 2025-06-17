# Lazada Thailand Daily Scraper

Simple daily scraper for Lazada Thailand product data. Saves to BigQuery automatically.

## 📋 Daily Checklist (5 minutes)

### Step 1: Get Fresh Cookies (Daily!)
1. Open Chrome → Go to https://www.lazada.co.th/
2. Search for a category (e.g., "hair care")
3. Press F12 → Network tab → Filter by "catalog"
4. Copy the curl command [Right click on the network request, click on copy as curl] → Save to `curl.txt`

### Step 2: Run Scraper
```bash
python scrape.py
```

### Step 3: Confirm Success
- Check terminal shows "✅ Uploaded X products to BigQuery"
- Verify data in BigQuery Console

## 📊 What It Does

- Scrapes 22 product categories (4,000+ products)
- Checks for duplicates (won't re-scrape same day)
- Saves to BigQuery + CSV backups
- Takes ~15 minutes to complete

## 🚨 If Something Breaks

**"Curl validation failed"** → Get fresh curl.txt (Step 1)  
**"Already scraped today"** → Type 'y' to continue anyway  
**"BigQuery error"** → Check with developer

## 🎯 Expected Output

```
✅ Scraping complete!
   📊 22/22 categories successful
   📦 4,362 total products scraped
   🗄️  Data uploaded to BigQuery
```

**That's it!** 🚀 