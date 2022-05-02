import json
import logging
import logging.handlers
import sys


def dump_as_json(d: dict):
    return json.dumps(d, separators=(",", ":"))


class ColorfulFormatter(logging.Formatter):
    GREY = "\x1b[38;21m"
    YELLOW = "\x1b[33;21m"
    RED = "\x1b[31;21m"
    BOLD_RED = "\x1b[31;1m"
    RESET = "\x1b[0m"
    FORMAT = "%(asctime)s [%(levelname).1s] %(process)s %(module)s: %(message)s"

    FORMATS = {
        logging.DEBUG: GREY + FORMAT + RESET,
        logging.INFO: GREY + FORMAT + RESET,
        logging.WARNING: YELLOW + FORMAT + RESET,
        logging.ERROR: RED + FORMAT + RESET,
        logging.CRITICAL: BOLD_RED + FORMAT + RESET
    }

    def format(self, record: logging.LogRecord):
        formatter = logging.Formatter(self.FORMATS.get(record.levelno, logging.INFO))
        return formatter.format(record)


def create_logging_handlers():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColorfulFormatter())

    return console_handler,
