#!/usr/bin/env python3
"""
Test suite for verify.py
========================
Tests the intern-friendly verification dashboard
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
from datetime import date
from io import StringIO

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import verify

class TestVerifyFunctions(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.expected_categories = {
            "Hair Care": "hair care",
            "Skin Care": "skin care", 
            "Electronics": "electronics",
            "Fashion": "fashion"
        }
        
    def test_get_status_emoji_perfect(self):
        """Test status emoji for perfect scraping"""
        emoji, status = verify.get_status_emoji(
            categories_done=22, 
            total_products=1100, 
            missing_cats=[], 
            low_cats=[], 
            expected_total=22
        )
        self.assertEqual(emoji, "✅")
        self.assertEqual(status, "OK")
    
    def test_get_status_emoji_with_issues(self):
        """Test status emoji when there are issues"""
        emoji, status = verify.get_status_emoji(
            categories_done=20, 
            total_products=900, 
            missing_cats=["Hair Care"], 
            low_cats=[("Electronics", 25)], 
            expected_total=22
        )
        self.assertEqual(emoji, "⚠️")
        self.assertEqual(status, "WARN")
    
    def test_get_status_emoji_no_data(self):
        """Test status emoji when no data exists"""
        emoji, status = verify.get_status_emoji(
            categories_done=0, 
            total_products=0, 
            missing_cats=["Hair Care", "Electronics"], 
            low_cats=[], 
            expected_total=22
        )
        self.assertEqual(emoji, "❌")
        self.assertEqual(status, "FAIL")
    
    @patch('verify.date')
    def test_check_country_categories_success(self, mock_date):
        """Test successful category checking"""
        mock_date.today.return_value.strftime.return_value = "2025-06-19"
        
        # Mock BigQuery results
        mock_row1 = Mock()
        mock_row1.category_name = "Hair Care"
        mock_row1.product_count = 45
        
        mock_row2 = Mock()
        mock_row2.category_name = "Electronics" 
        mock_row2.product_count = 25  # Low count
        
        self.mock_client.query.return_value = [mock_row1, mock_row2]
        self.mock_client.project = "test-project"
        
        result = verify.check_country_categories(
            self.mock_client, "thailand", self.expected_categories, verbose=False
        )
        
        self.assertEqual(result['country_key'], "thailand")
        self.assertEqual(result['total_products'], 70)
        self.assertEqual(result['categories_done'], 2)
        self.assertEqual(len(result['missing_categories']), 2)  # Skin Care, Fashion missing
        self.assertEqual(len(result['low_count_categories']), 1)  # Electronics < 30
        self.assertEqual(result['low_count_categories'][0], ("Electronics", 25))
    
    @patch('verify.date')
    def test_check_country_categories_error(self, mock_date):
        """Test category checking with BigQuery error"""
        mock_date.today.return_value.strftime.return_value = "2025-06-19"
        
        self.mock_client.query.side_effect = Exception("BigQuery error")
        
        result = verify.check_country_categories(
            self.mock_client, "thailand", self.expected_categories, verbose=False
        )
        
        self.assertEqual(result['country_key'], "thailand")
        self.assertEqual(result['total_products'], 0)
        self.assertEqual(result['categories_done'], 0)
        self.assertEqual(len(result['missing_categories']), 4)  # All categories missing
        self.assertIn('error', result)
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_print_dashboard_success(self, mock_stdout):
        """Test dashboard printing for successful run"""
        country_results = {
            "thailand": {
                'country_key': "thailand",
                'total_products': 1100,
                'categories_done': 22,
                'missing_categories': [],
                'low_count_categories': []
            },
            "indonesia": {
                'country_key': "indonesia", 
                'total_products': 1050,
                'categories_done': 22,
                'missing_categories': [],
                'low_count_categories': []
            },
            "malaysia": {
                'country_key': "malaysia",
                'total_products': 980,
                'categories_done': 22, 
                'missing_categories': [],
                'low_count_categories': []
            }
        }
        
        issues_found, total_products, total_target = verify.print_dashboard(
            country_results, self.expected_categories, verbose=False
        )
        
        output = mock_stdout.getvalue()
        
        self.assertFalse(issues_found)
        self.assertEqual(total_products, 3130)
        self.assertIn("🔍 LAZADA DAILY CHECK", output)
        self.assertIn("🇹🇭 Thailand", output)
        self.assertIn("22/22", output)
        self.assertIn("✅", output)
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_print_dashboard_with_issues(self, mock_stdout):
        """Test dashboard printing with issues"""
        country_results = {
            "thailand": {
                'country_key': "thailand",
                'total_products': 850,
                'categories_done': 20,
                'missing_categories': ["Hair Care", "Electronics"],
                'low_count_categories': [("Fashion", 25)]
            },
            "indonesia": {
                'country_key': "indonesia",
                'total_products': 0,
                'categories_done': 0,
                'missing_categories': list(self.expected_categories.keys()),
                'low_count_categories': []
            },
            "malaysia": {
                'country_key': "malaysia",
                'total_products': 1100,
                'categories_done': 22,
                'missing_categories': [],
                'low_count_categories': []
            }
        }
        
        issues_found, total_products, total_target = verify.print_dashboard(
            country_results, self.expected_categories, verbose=False
        )
        
        output = mock_stdout.getvalue()
        
        self.assertTrue(issues_found)
        self.assertEqual(total_products, 1950)
        self.assertIn("⚠️", output)
        self.assertIn("❌", output)
        self.assertIn("issues", output)
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_print_next_steps_all_good(self, mock_stdout):
        """Test next steps when everything is perfect"""
        country_results = {
            "thailand": {
                'missing_categories': [],
                'low_count_categories': []
            }
        }
        
        all_good = verify.print_next_steps(country_results, verbose=False)
        output = mock_stdout.getvalue()
        
        self.assertTrue(all_good)
        self.assertIn("🎉 All good – nothing to do!", output)
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_print_next_steps_with_issues(self, mock_stdout):
        """Test next steps when there are issues"""
        country_results = {
            "thailand": {
                'missing_categories': ["Hair Care"],
                'low_count_categories': [("Electronics", 25)]
            },
            "indonesia": {
                'missing_categories': [],
                'low_count_categories': [("Fashion", 20), ("Skin Care", 15)]
            }
        }
        
        all_good = verify.print_next_steps(country_results, verbose=False)
        output = mock_stdout.getvalue()
        
        self.assertFalse(all_good)
        self.assertIn("📋 ISSUES FOUND:", output)
        self.assertIn("🇹🇭 Thailand: 1 missing categories", output)
        self.assertIn("🇹🇭 Thailand: 1 categories < 30 products", output) 
        self.assertIn("🇮🇩 Indonesia: 2 categories < 30 products", output)
        self.assertIn("Get fresh curl files", output)
    
    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='{"Hair Care": "hair care", "Electronics": "electronics"}')
    def test_load_categories(self, mock_open):
        """Test loading categories from JSON"""
        categories = verify.load_categories()
        
        self.assertEqual(len(categories), 2)
        self.assertIn("Hair Care", categories)
        self.assertEqual(categories["Hair Care"], "hair care")
    
    @patch('builtins.open', side_effect=FileNotFoundError)
    @patch('sys.stdout', new_callable=StringIO)
    def test_load_categories_file_not_found(self, mock_stdout, mock_open):
        """Test loading categories when file doesn't exist"""
        categories = verify.load_categories()
        output = mock_stdout.getvalue()
        
        self.assertEqual(categories, {})
        self.assertIn("Cannot load categories.json", output)
    
    @patch('verify.init_bigquery')
    @patch('verify.load_categories')
    def test_main_success_path(self, mock_load_categories, mock_init_bq):
        """Test main function success path"""
        mock_load_categories.return_value = self.expected_categories
        mock_init_bq.return_value = self.mock_client
        
        # Mock successful category checking
        with patch('verify.check_country_categories') as mock_check:
            mock_check.return_value = {
                'country_key': 'thailand',
                'total_products': 1100,
                'categories_done': 22,
                'missing_categories': [],
                'low_count_categories': []
            }
            
            with patch('verify.print_dashboard') as mock_dashboard:
                mock_dashboard.return_value = (False, 3300, 3300)  # no issues, perfect score
                
                with patch('verify.print_next_steps') as mock_next_steps:
                    mock_next_steps.return_value = True  # all good
                    
                    with patch('sys.argv', ['verify.py']):
                        with self.assertRaises(SystemExit) as cm:
                            verify.main()
                        
                        self.assertEqual(cm.exception.code, 0)  # Success exit code


class TestVerifyIntegration(unittest.TestCase):
    """Integration tests with real-ish data"""
    
    def setUp(self):
        self.expected_categories = {
            "Hair Care": "hair care",
            "Skin Care": "skin care",
            "Electronics": "electronics", 
            "Fashion": "fashion",
            "Baby Products": "baby products"
        }
    
    def test_integration_perfect_scenario(self):
        """Test perfect scenario end-to-end"""
        # Mock client with perfect data
        mock_client = Mock()
        mock_client.project = "test-project"
        
        # Create perfect mock results
        perfect_results = []
        for category in self.expected_categories.keys():
            mock_row = Mock()
            mock_row.category_name = category
            mock_row.product_count = 50  # Perfect count
            perfect_results.append(mock_row)
        
        mock_client.query.return_value = perfect_results
        
        # Test all countries
        with patch('verify.date') as mock_date:
            mock_date.today.return_value.strftime.return_value = "2025-06-19"
            
            results = {}
            for country in ["thailand", "indonesia", "malaysia"]:
                results[country] = verify.check_country_categories(
                    mock_client, country, self.expected_categories, verbose=False
                )
            
            # All should be perfect
            for country, result in results.items():
                self.assertEqual(result['total_products'], 250)  # 5 categories * 50 products
                self.assertEqual(result['categories_done'], 5)
                self.assertEqual(len(result['missing_categories']), 0)
                self.assertEqual(len(result['low_count_categories']), 0)
    
    def test_integration_problematic_scenario(self):
        """Test scenario with various issues"""
        mock_client = Mock()
        mock_client.project = "test-project"
        
        # Create problematic mock results - missing categories and low counts
        problematic_results = [
            Mock(category_name="Hair Care", product_count=25),  # Low count
            Mock(category_name="Electronics", product_count=45),  # Good
            # Missing: Skin Care, Fashion, Baby Products
        ]
        
        mock_client.query.return_value = problematic_results
        
        with patch('verify.date') as mock_date:
            mock_date.today.return_value.strftime.return_value = "2025-06-19"
            
            result = verify.check_country_categories(
                mock_client, "thailand", self.expected_categories, verbose=False
            )
            
            self.assertEqual(result['total_products'], 70)
            self.assertEqual(result['categories_done'], 2)
            self.assertEqual(len(result['missing_categories']), 3)  # 3 missing
            self.assertEqual(len(result['low_count_categories']), 1)  # Hair Care < 30
            
            # Check specific issues
            self.assertIn("Skin Care", result['missing_categories'])
            self.assertIn("Fashion", result['missing_categories']) 
            self.assertIn("Baby Products", result['missing_categories'])
            self.assertEqual(result['low_count_categories'][0], ("Hair Care", 25))


if __name__ == '__main__':
    # Run tests
    unittest.main(verbosity=2) 