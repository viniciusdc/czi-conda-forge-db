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


import logging
from pathlib import Path
from datetime import datetime
from rich.logging import RichHandler


class CFLogger:
    def __init__(self, logger_name="CondaForgeDB", log_file: Path = None):
        self.log_file = self._set_log_file(log_file)
        self.logger = self.setup_logger(logger_name)

    def __repr__(self) -> str:
        return f"CFLogger({self.logger.name})"

    def _set_log_file(self, log_file: Path):
        if not log_file:
            _now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            return Path(__name__).parent / f".{_now}.log"
        elif not isinstance(log_file, Path):
            raise TypeError(
                "log_file must be a pathlib.Path object not {type(log_file)}"
            )
        elif not log_file.parent.exists():
            raise FileNotFoundError(
                f"{log_file.parent.absolute().as_posix()} does not exist"
            )
        else:
            return log_file

    def setup_logger(self, logger_name):
        # Set up logging

        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)

        # Create a file handler
        file_handler = logging.FileHandler(filename=self.log_file.as_posix())
        file_handler.setLevel(logging.DEBUG)

        # Create a rich handler
        rich_handler = RichHandler()
        rich_handler.setLevel(logging.INFO)

        # Create a formatter
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )

        # Set the formatter for the file handler
        file_handler.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(rich_handler)

        return logger

    def _remove_handlers(self):
        # remove handlers from logger, only used for testing
        while len(self.logger.handlers) > 0:
            self.logger.removeHandler(self.logger.handlers[0])


logger = CFLogger().logger

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
