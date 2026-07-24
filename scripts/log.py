import logging

from rich.logging import RichHandler

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.DEBUG,
    handlers=[RichHandler()],
)

log = logging.getLogger("rich")
