import logging


class ColorCodes:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    RESET = '\033[0m'


class ColoredFormatter(logging.Formatter):
    """Formatter für farbiges Logging"""

    def format(self, record):
        formatted_msg = super().format(record)
        if record.levelno >= logging.ERROR:
            color = ColorCodes.ERROR
        elif record.levelno >= logging.WARNING:
            color = ColorCodes.WARNING
        elif record.levelno >= logging.INFO:
            color = ColorCodes.GREEN  # Changed to GREEN for INFO level
        else:
            color = ColorCodes.BLUE
        return f"{color}{formatted_msg}{ColorCodes.RESET}"


def setup_logger(name: str) -> logging.Logger:
    """Konfiguriert und gibt einen Logger zurück"""
    logger = logging.getLogger(name)

    # Remove all handlers to prevent duplication
    if logger.handlers:
        logger.handlers.clear()

    handler = logging.StreamHandler()
    formatter = ColoredFormatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Prevent propagation to avoid duplicate logs
    logger.propagate = False

    return logger
