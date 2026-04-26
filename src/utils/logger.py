import logging

_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def get_logger(name: str) -> logging.Logger:
    # Lazy initialization of logging configuration - only set up logging if it hasn't been configured yet
    if not logging.root.handlers:
        logging.basicConfig(level=logging.INFO, format=_FORMAT)
    return logging.getLogger(name)
