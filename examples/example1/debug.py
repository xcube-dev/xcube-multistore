import logging
import sys

from xcube_multistore import MultiSourceDataStore

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("my_log.log"),
        logging.StreamHandler(sys.stdout),  # stdout
    ],
    force=True,
)

# Force all existing loggers
for name, logger in logging.root.manager.loggerDict.items():
    if isinstance(logger, logging.Logger):
        logger.setLevel(logging.INFO)
        logger.propagate = True

msds = MultiSourceDataStore("scs1_config.yml")
msds.generate()
