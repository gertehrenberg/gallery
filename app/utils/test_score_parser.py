# app/utils/test_score_parser.py

import unittest
from unittest.mock import patch

from app.utils.score_parser import check_image_scores


class TestScoreParser(unittest.TestCase):

    @patch('app.utils.score_parser.load_scores_from_db')
    def test_named_keys_expression_true(self, mock_load):
        mock_load.return_value = {
            'porn': 80,
            'nsfw_score': 40,
            'hentai': 20
        }
        expr = "porn >= 50 AND nsfw_score < 70 AND hentai <= 30"
        result = check_image_scores("dummy.db", "test.jpg", expr)
        self.assertTrue(result)

    @patch('app.utils.score_parser.load_scores_from_db')
    def test_named_keys_expression_false(self, mock_load):
        mock_load.return_value = {
            'porn': 40,
            'nsfw_score': 90,
            'drawings': 10
        }
        expr = "porn >= 50 AND nsfw_score < 70 AND drawings > 5"
        result = check_image_scores("dummy.db", "test.jpg", expr)
        self.assertFalse(result)

    @patch('app.utils.score_parser.load_scores_from_db')
    def test_named_keys_missing_key(self, mock_load):
        mock_load.return_value = {
            'porn': 60,
        }
        expr = "porn > 50 AND hentai < 10"
        with self.assertRaises(KeyError):
            check_image_scores("dummy.db", "test.jpg", expr)

    @patch('app.utils.score_parser.load_scores_from_db')
    def test_named_keys_invalid_key(self, mock_load):
        mock_load.return_value = {
            'porn': 70,
        }
        expr = "unknown_score > 0"
        with self.assertRaises(ValueError):
            check_image_scores("dummy.db", "test.jpg", expr)

if __name__ == '__main__':
    unittest.main()
