import logging
from pathlib import Path

from xcube_multistore.multistore import MultiSourceDataStore

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
)


DIR = Path(__file__).parent.resolve()
msds = MultiSourceDataStore(DIR / "config.yml")
