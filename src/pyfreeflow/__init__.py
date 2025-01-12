import importlib
import logging
import pyfreeflow.ext
import pyfreeflow.pipeline
from sys import version_info


handler = logging.StreamHandler()
if version_info.major > 3 or (version_info.major == 3 and
                              version_info.minor > 11):
    formatter = logging.Formatter('[%(asctime)s] %(name)s - TaskName[%(taskName)s] - %(levelname)s - %(message)s')
else:
    formatter = logging.Formatter('[%(asctime)s] %(name)s - %(levelname)s - %(message)s')

handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)
logger.propagate = True
logger.addHandler(handler)


def load_extension(ext_name):
    logger.info("Loading extension: %s", ext_name)
    importlib.import_module(ext_name)
    logger.info("Loaded extension: %s", ext_name)


def set_loglevel(level):
    logger.setLevel(level)


def get_logformat():
    return formatter._fmt
