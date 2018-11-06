import logging
from colorlog import ColoredFormatter


def get_logger(logger_name, log_level):
    LOG_LEVEL = logging.getLevelName(log_level)
    LOGFORMAT = "%(log_color)s%(asctime)s %(levelname)s: %(module_name)s%(reset)s | %(log_color)s%(message)s%(reset)s"  # NOQA
    extra = {'module_name': logger_name}

    logging.root.setLevel(LOG_LEVEL)
    formatter = ColoredFormatter(LOGFORMAT)
    stream = logging.StreamHandler()
    stream.setLevel(LOG_LEVEL)
    stream.setFormatter(formatter)
    logger = logging.getLogger(logger_name)
    logger.setLevel(LOG_LEVEL)
    logger.addHandler(stream)
    logger = logging.LoggerAdapter(logger, extra)
    return logger
