# Lazada Thailand Daily Scraper

Simple daily scraper for Lazada Thailand product data. Saves to BigQuery automatically.

## 🔧 One-Time Setup (First Time Only)

**Before first run**: Email `nilesh@pollen.tech` to get the `key.json` file.
- Save it in the root directory of this project
- This file is needed for BigQuery authentication
- Only needed once per setup

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

## ⚠️ IMPORTANT WARNING

**If running multiple times per day**: Always get fresh cookies (Step 1) before each run!

- Reusing old cookies can trigger anti-bot protection
- You may get IP banned or blocked temporarily
- Each run should use a fresh browser session and new curl command
- Wait at least 1-2 hours between runs to be safe

## 📊 What It Does

- Scrapes 22 product categories (4,000+ products)
- Checks for duplicates (won't re-scrape same day)
- Saves to BigQuery + CSV backups
- Takes ~15 minutes to complete

## 🚨 If Something Breaks

**"key.json not found"** → Email nilesh@pollen.tech for the file  
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