import logging

import pytest
from rich.logging import RichHandler

from cfdb.log.setup import CFLogger


@pytest.fixture
def cf_logger(tmp_path):
    logger = CFLogger(logger_name="TestLogger", log_file=tmp_path / "test.log")
    yield logger
    logger._remove_handlers()


def test_logger_initialization(cf_logger):
    logger = cf_logger.logger
    assert isinstance(logger, logging.Logger)
    assert logger.name == "TestLogger"
    assert cf_logger.log_file is not None
    assert cf_logger.log_file.exists()


def test_number_of_handlers(cf_logger):
    logger = cf_logger.logger
    assert len(logger.handlers) == 2


def test_logger_file_handler(cf_logger):
    logger = cf_logger.logger
    file_handler = cf_logger.logger.handlers[0]
    assert isinstance(file_handler, logging.FileHandler)
    assert file_handler.baseFilename == cf_logger.log_file.absolute().as_posix()


def test_logger_rich_handler(cf_logger):
    logger = cf_logger.logger
    has_rich_handler = any(
        isinstance(handler, RichHandler) for handler in logger.handlers
    )
    assert has_rich_handler


def test_logger_file_path_exists(cf_logger):
    assert cf_logger.log_file.exists()


def test_logger_file_path_persistence(cf_logger):
    log_file = cf_logger.log_file
    del cf_logger
    assert log_file.exists()
