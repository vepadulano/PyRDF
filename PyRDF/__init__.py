"""
Top level functions and variables of the PyRDF package
"""
import logging
import sys

from PyRDF import Backends

logger = logging.getLogger(__name__)


def create_logger(level="WARNING", log_path="./PyRDF.log"):
    """PyRDF basic logger"""
    logger = logging.getLogger(__name__)

    level = getattr(logging, level)

    logger.setLevel(level)

    format_string = ("%(levelname)s: %(name)s[%(asctime)s]: %(message)s")
    formatter = logging.Formatter(format_string)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_path:
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def initialize(fun, *args, **kwargs):
    """
    Set a function that will be executed as a first step on every backend before
    any other operation. This method also executes the function on the current
    user environment so changes are visible on the running session.

    This allows users to inject and execute custom code on the worker
    environment without being part of the RDataFrame computational graph.

    Args:
        fun (function): Function to be executed.

        *args (list): Variable length argument list used to execute the
            function.

        **kwargs (dict): Keyword arguments used to execute the function.
    """
    Backends.Base.BaseBackend.register_initialization(fun, *args, **kwargs)
