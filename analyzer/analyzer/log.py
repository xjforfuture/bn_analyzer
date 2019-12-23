
import logging

from . import config

dbg_dict = {
    'CRITICAL':logging.CRITICAL,
    'FATAL':logging.FATAL,
    'ERROR':logging.ERROR,
    'WARNING':logging.WARNING,
    'WARN':logging.WARN,
    'INFO':logging.INFO,
    'DEBUG':logging.DEBUG,
    'NOTSET':logging.NOTSET,
}

logger = None


def log_init(module: str, outfile: str):
    global logger
    logger = logging.getLogger(module)
    handler = logging.FileHandler(outfile)
    fmt = logging.Formatter(fmt='%(asctime)s - %(levelname)s: %(filename)s-L%(lineno)d  %(message)s',
                            datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    logger.setLevel(dbg_dict.get(config.CFG.get('log_level')))
