import logging
from logging.handlers import RotatingFileHandler


handlers= RotatingFileHandler(
    filename = "app.log",
    maxBytes=1_000_000,  # 1 MB
    backupCount=7,
    encoding="utf-8"
)

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s | %(filename)s | %(levelname)s | %(message)s",
    handlers=[handlers]

)

log = logging.getLogger()