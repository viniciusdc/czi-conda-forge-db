import logging
from datetime import datetime
from pathlib import Path

from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)


def logging_setup():
    # set up logging
    log_file = str(Path(__name__).parent / f".{datetime.now()}.log")
    logger = logging.getLogger("")
    logger.addHandler(RichHandler())
    logger.addHandler(logging.FileHandler(log_file))
    logger.setLevel(logging.DEBUG)
    return logger


logger = logging_setup()

# set up progress bar columns for rich progress bar
progressBar = Progress(
    TextColumn("[progress.description]{task.description}"),
    BarColumn(
        bar_width=None,
        pulse_style="bright_black",
    ),
    TaskProgressColumn(),
    TimeRemainingColumn(),
    MofNCompleteColumn(),
    expand=True,
)
