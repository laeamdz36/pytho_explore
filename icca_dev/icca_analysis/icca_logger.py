"""Logger to the module"""

import logging
import sys

def build_main_logger():
    """Build the main logger"""

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)-10s : %(levelname)-10s:line %(lineno)-4s: %(message)-50s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    
    logger.addHandler(ch)

    return logger