#!/usr/bin/env python3
"""
Test suite for scraper integration with verify
==============================================
Tests that scraper properly runs verification at the end
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
from io import StringIO

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import scrape

class TestScraperVerifyIntegration(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.expected_categories = {
            "Hair Care": "hair care",
            "Electronics": "electronics"
        }
    
    @patch('scrape.init_bigquery')
    @patch('scrape.check_vpn')
    @patch('scrape.load_categories')
    @patch('scrape.scrape_country')
    @patch('scrape.load_categories_verify')
    @patch('scrape.check_country_categories')
    @patch('scrape.print_dashboard')
    @patch('scrape.print_next_steps')
    @patch('sys.stdout', new_callable=StringIO)
    def test_main_with_verification(self, mock_stdout, mock_next_steps, mock_dashboard, 
                                  mock_check_categories, mock_load_verify, mock_scrape_country,
                                  mock_load_categories, mock_check_vpn, mock_init_bq):
        """Test main function includes verification at the end"""
        
        # Mock successful setup
        mock_check_vpn.return_value = (True, "US")
        mock_load_categories.return_value = self.expected_categories
        mock_init_bq.return_value = self.mock_client
        
        # Mock successful scraping for all countries
        mock_scrape_country.return_value = (True, 22, 1100)  # success, categories, products
        
        # Mock verification functions
        mock_load_verify.return_value = self.expected_categories
        mock_check_categories.return_value = {
            'country_key': 'thailand',
            'total_products': 1100,
            'categories_done': 22,
            'missing_categories': [],
            'low_count_categories': []
        }
        mock_dashboard.return_value = (False, 3300, 3300)  # no issues, perfect score
        mock_next_steps.return_value = True  # all good
        
        # Run main function
        scrape.main()
        
        # Verify verification functions were called
        mock_load_verify.assert_called_once()
        self.assertEqual(mock_check_categories.call_count, 3)  # Called for each country
        mock_dashboard.assert_called_once()
        mock_next_steps.assert_called_once()
        
        # Check output includes verification section
        output = mock_stdout.getvalue()
        self.assertIn("🔍 VERIFICATION CHECK", output)
    
    @patch('scrape.init_bigquery')
    @patch('scrape.check_vpn')
    @patch('scrape.load_categories')
    @patch('scrape.scrape_country')
    @patch('scrape.load_categories_verify')
    @patch('sys.stdout', new_callable=StringIO)
    def test_verification_failure_handling(self, mock_stdout, mock_load_verify, 
                                         mock_scrape_country, mock_load_categories, 
                                         mock_check_vpn, mock_init_bq):
        """Test verification failure is handled gracefully"""
        
        # Mock successful setup and scraping
        mock_check_vpn.return_value = (True, "US")
        mock_load_categories.return_value = self.expected_categories
        mock_init_bq.return_value = self.mock_client
        mock_scrape_country.return_value = (True, 22, 1100)
        
        # Mock verification failure
        mock_load_verify.side_effect = Exception("Verification error")
        
        # Run main function - should not crash
        scrape.main()
        
        # Check output includes error message and fallback instruction
        output = mock_stdout.getvalue()
        self.assertIn("🔍 VERIFICATION CHECK", output)
        self.assertIn("Verification check failed", output)
        self.assertIn("Run 'python verify.py' manually", output)
    
    @patch('scrape.init_bigquery')
    @patch('scrape.check_vpn')
    @patch('scrape.load_categories')
    @patch('scrape.scrape_country')
    @patch('sys.stdout', new_callable=StringIO)
    def test_verification_skipped_when_no_bigquery(self, mock_stdout, mock_scrape_country,
                                                 mock_load_categories, mock_check_vpn, 
                                                 mock_init_bq):
        """Test verification is skipped when BigQuery unavailable"""
        
        # Mock successful setup but no BigQuery
        mock_check_vpn.return_value = (True, "US")
        mock_load_categories.return_value = self.expected_categories
        mock_init_bq.return_value = None  # No BigQuery client
        mock_scrape_country.return_value = (True, 22, 1100)
        
        # Run main function
        scrape.main()
        
        # Check output shows verification was skipped
        output = mock_stdout.getvalue()
        self.assertIn("🔍 VERIFICATION CHECK", output)
        self.assertIn("Verification skipped", output)


class TestScraperHelperFunctions(unittest.TestCase):
    """Test scraper helper functions that might be affected by verify integration"""
    
    def test_verify_import_handles_missing_module(self):
        """Test that scraper handles missing verify module gracefully"""
        # This test ensures that if verify.py is missing, scraper doesn't crash
        
        # Mock the import to fail
        with patch('builtins.__import__', side_effect=ImportError("No module named 'verify'")):
            # Should not raise exception when importing scrape module
            try:
                import importlib
                importlib.reload(scrape)  # Reload to trigger import error
            except ImportError:
                self.fail("Scraper should handle missing verify module gracefully")


class TestEndToEndWorkflow(unittest.TestCase):
    """End-to-end workflow tests"""
    
    @patch('scrape.subprocess.run')
    @patch('scrape.init_bigquery')
    @patch('scrape.check_vpn')
    @patch('scrape.load_categories')
    @patch('scrape.load_curl_command')
    @patch('scrape.validate_curl_command')
    @patch('scrape.check_today_run')
    @patch('scrape.run_curl_for_query')
    @patch('scrape.parse_products')
    @patch('scrape.upload_to_bigquery')
    @patch('scrape.save_to_csv')
    @patch('scrape.load_categories_verify')
    @patch('scrape.check_country_categories')
    @patch('scrape.print_dashboard')
    @patch('scrape.print_next_steps')
    def test_complete_workflow_with_verification(self, mock_next_steps, mock_dashboard,
                                               mock_check_categories, mock_load_verify,
                                               mock_save_csv, mock_upload_bq, mock_parse,
                                               mock_curl, mock_check_today, mock_validate,
                                               mock_load_curl, mock_load_categories,
                                               mock_check_vpn, mock_init_bq, mock_subprocess):
        """Test complete workflow from start to verification"""
        
        # Mock all the setup
        mock_check_vpn.return_value = (True, "US")
        mock_load_categories.return_value = {"Hair Care": "hair care"}
        mock_init_bq.return_value = Mock()
        mock_load_curl.return_value = "curl 'http://example.com'"
        mock_validate.return_value = True
        mock_check_today.return_value = False  # Not scraped today
        
        # Mock scraping process
        mock_curl.return_value = {"mods": {"listItems": [{"name": "Test Product"}]}}
        mock_parse.return_value = [{"name": "Test Product", "price": "100"}]
        mock_upload_bq.return_value = True
        mock_save_csv.return_value = "test.csv"
        
        # Mock verification
        mock_load_verify.return_value = {"Hair Care": "hair care"}
        mock_check_categories.return_value = {
            'country_key': 'thailand',
            'total_products': 50,
            'categories_done': 1,
            'missing_categories': [],
            'low_count_categories': []
        }
        mock_dashboard.return_value = (False, 150, 150)  # no issues
        mock_next_steps.return_value = True
        
        # Mock user inputs to proceed with scraping
        with patch('builtins.input', return_value='y'):
            # Run the scraper
            scrape.main()
        
        # Verify that verification was called at the end
        mock_load_verify.assert_called()
        mock_dashboard.assert_called()
        mock_next_steps.assert_called()


if __name__ == '__main__':
    # Run tests
    unittest.main(verbosity=2) 