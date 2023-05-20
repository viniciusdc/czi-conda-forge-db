import logging
from pathlib import Path
from rich.logging import RichHandler


def logging_setup():
    # set up logging
    log_file = str(Path(__name__).parent / "database.log")
    logger = logging.getLogger("")
    logger.addHandler(RichHandler())
    logger.addHandler(logging.FileHandler(log_file))
    logger.setLevel(logging.DEBUG)
    return logger


logger = logging_setup()
