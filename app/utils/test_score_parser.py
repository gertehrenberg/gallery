import unittest
from unittest.mock import patch

from ..config import score_type_map
from ..utils.score_parser import check_image_scores
from ..utils.score_parser import parse_score_expression

dummy_scores = {key: 0 for key in score_type_map.keys()}


class TestScoreParser(unittest.TestCase):

    @patch('app.utils.score_parser.load_scores_from_db')
    def test_simple1(self, mock_load):
        mock_load.return_value = {
            'nsfw_score': 40
        }
        expr = "nsfw_score > 3"
        try:
            result = check_image_scores("dummy.db", "test.jpg", expr)
            self.assertTrue(result)
        except Exception as e:
            self.fail(f"Unexpected exception raised: {e}")

    @patch('app.utils.score_parser.load_scores_from_db')
    def test_simple2(self, mock_load):
        mock_load.return_value = {
            'nsfw_score': 1
        }
        expr = "nsfw_score > 3"
        try:
            result = check_image_scores("dummy.db", "test.jpg", expr)
            self.assertFalse(result)
        except Exception as e:
            self.fail(f"Unexpected exception raised: {e}")

    @patch('app.utils.score_parser.load_scores_from_db')
    def test_simple3(self, mock_load):
        mock_load.return_value = {
            'text': 2000
        }
        expr = "text < 1000"
        try:
            result = check_image_scores("dummy.db", "test.jpg", expr)
            self.assertFalse(result)
        except Exception as e:
            self.fail(f"Unexpected exception raised: {e}")

    @patch('app.utils.score_parser.load_scores_from_db')
    def test_named_keys_expression_true(self, mock_load):
        mock_load.return_value = {
            'porn': 80,
            'nsfw_score': 40,
            'hentai': 20
        }
        expr = "porn >= 50 AND nsfw_score < 70 AND hentai <= 30"
        try:
            result = check_image_scores("dummy.db", "test.jpg", expr)
            self.assertTrue(result)
        except Exception as e:
            self.fail(f"Unexpected exception raised: {e}")

    @patch('app.utils.score_parser.load_scores_from_db')
    def test_named_keys_expression_false(self, mock_load):
        mock_load.return_value = {
            'porn': 40,
            'nsfw_score': 90,
            'drawings': 10
        }
        expr = "porn >= 50 AND nsfw_score < 70 AND drawings > 5"
        try:
            result = check_image_scores("dummy.db", "test.jpg", expr)
            self.assertFalse(result)
        except Exception as e:
            self.fail(f"Unexpected exception raised: {e}")

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

    @patch('app.utils.score_parser.load_scores_from_db')
    def test_text(self, mock_load):
        mock_load.return_value = {
            'text': 30,
        }
        expr = "text < 50"
        try:
            result = check_image_scores("dummy.db", "test.jpg", expr)
            self.assertTrue(result)
        except Exception as e:
            self.fail(f"Unexpected exception raised: {e}")

    def test_parse_expression_error(self):
        expr = "nsfw_score >>> 50"  # Invalid syntax
        with self.assertRaises(ValueError):
            parse_score_expression(expr, dummy_scores)


if __name__ == '__main__':
    unittest.main()
