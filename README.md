# Lazada Multi-Country Daily Scraper

Simple daily scraper for Lazada Thailand 🇹🇭, Indonesia 🇮🇩, and Malaysia 🇲🇾 product data. Saves to BigQuery automatically.

## 🔧 One-Time Setup (First Time Only)

**Before first run**: Email `nilesh@pollen.tech` to get the `key.json` file.
- Save it in the root directory of this project
- This file is needed for BigQuery authentication
- Only needed once per setup

## 📋 Daily Checklist (5 minutes)

### Step 1: Get Fresh Cookies (Daily!)

**Thailand** 🇹🇭:
1. Open Chrome → Go to https://www.lazada.co.th/
2. Search for a category (e.g., "hair care")
3. Press F12 → Network tab → Filter by "catalog"
4. Copy the curl command [Right click on the network request, click on copy as curl] → Save to `curl_th.txt`

**Indonesia** 🇮🇩:
1. Open Chrome → Go to https://www.lazada.co.id/
2. Search for a category (e.g., "hair care") 
3. Press F12 → Network tab → Filter by "tag"
4. Copy the curl command [Right click on the network request, click on copy as curl] → Save to `curl_id.txt`

**Malaysia** 🇲🇾:
1. Open Chrome → Go to https://www.lazada.com.my/
2. Search for a category (e.g., "hair care")
3. Press F12 → Network tab → Filter by "tag" 
4. Copy the curl command [Right click on the network request, click on copy as curl] → Save to `curl_ml.txt`

### Step 2: Run Scraper
```bash
python scrape.py
```

### Step 3: Confirm Success
- Check terminal shows "✅ Multi-country scraping complete!"
- Verify data with `python verify.py`

## 🔍 Quick Data Check

**Want to see if scraping already ran today?**
```bash
python verify.py
```

Shows:
- ✅ Did scraping run today for each country?
- 📊 How many products per category per country
- 📈 Data quality assessment and historical trends
- 🎯 Progress towards daily targets

Perfect for checking before running the scraper!

## ⚠️ IMPORTANT WARNING

**If running multiple times per day**: Always get fresh cookies (Step 1) before each run!

- Reusing old cookies can trigger anti-bot protection
- You may get IP banned or blocked temporarily  
- Each run should use a fresh browser session and new curl commands
- Wait at least 1-2 hours between runs to be safe
- **VPN REQUIRED**: Connect to VPN (any non-India location) before starting

## 📊 What It Does

- Scrapes 3 countries: Thailand → Indonesia → Malaysia
- 22 product categories per country (~3,300 total products)
- Checks for duplicates (won't re-scrape same day)
- Saves to BigQuery + CSV backups
- Takes ~15-20 minutes to complete all countries
- Smart resume: If already scraped today, asks if you want to continue

## 🚨 If Something Breaks

**"key.json not found"** → Email nilesh@pollen.tech for the file  
**"Curl validation failed"** → Get fresh curl files (Step 1)  
**"CAPTCHA detected"** → Get fresh curl files after solving robot challenges  
**"Already scraped today"** → Type 'n' to skip or 'y' to re-scrape  
**"VPN REQUIRED"** → Connect to VPN (any location outside India)  
**"BigQuery error"** → Check with developer

## 🎯 Expected Output

```
🎉 MULTI-COUNTRY SCRAPING COMPLETE!
🇹🇭 Thailand: 22 categories, 1100 products
🇮🇩 Indonesia: 22 categories, 1100 products  
🇲🇾 Malaysia: 22 categories, 1100 products
📦 Total: 3300 products
🗄️  All data uploaded to BigQuery
```

**That's it!** 🚀 