import re
from app.config import score_type_map, reverse_score_type_map
from app.database import load_scores_from_db


def parse_score_expression(expr, values):
    """
    Prüft einen Ausdruck wie 'porn > 50 AND nsfw_score <= 70' gegen ein values-Dict.
    """
    allowed_keys = set(score_type_map.keys())
    condition_pattern = r"([a-zA-Z_0-9]+)\s*(==|!=|<=|>=|<|>)\s*(\d+)"
    tokens = re.split(r"\s*(?:&|;|AND)\s*", expr, flags=re.IGNORECASE)

    ops = {
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "<=": lambda a, b: a <= b,
        ">=": lambda a, b: a >= b,
        "<": lambda a, b: a < b,
        ">": lambda a, b: a > b,
    }

    for token in tokens:
        token = token.strip()
        match = re.fullmatch(condition_pattern, token)
        if not match:
            raise ValueError(f"Ungültiger Ausdruck: {token}")

        key, op, threshold_str = match.groups()
        threshold = int(threshold_str)

        if key not in allowed_keys:
            raise ValueError(f"Unbekannter Score-Schlüssel: {key} (erlaubt: {', '.join(sorted(allowed_keys))})")

        value = values.get(key)
        if value is None:
            raise KeyError(f"{key} fehlt in Werten")

        if not ops[op](value, threshold):
            return False

    return True


def check_image_scores(db_path, image_name, condition_expr):
    """
    Lädt die Scores eines einzelnen Bildes aus der DB und prüft, ob der Ausdruck zutrifft.
    """
    scores = load_scores_from_db(db_path, image_name)
    return parse_score_expression(condition_expr, scores)
