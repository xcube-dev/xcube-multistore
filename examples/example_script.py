import logging
from pathlib import Path
import os

from ms_datastore.ms_datastore import MultiSourceDataStore


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
)


DIR = Path(__file__).parent.resolve()
msds = MultiSourceDataStore.from_file(os.path.join(DIR, "config.yml"), visualize=False)
