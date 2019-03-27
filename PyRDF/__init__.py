from .RDataFrame import RDataFrame  # noqa
from .RDataFrame import RDataFrameException  # noqa
from .CallableGenerator import CallableGenerator  # noqa
from backend.Local import Local
from backend.Backend import Backend
from backend.Utils import Utils

current_backend = Local()
includes = []


def use(backend_name, conf={}):
    """
    Allows the user to choose the execution backend.

    Parameters
    ----------
    backend_name : str
        This is the name of the chosen backend.

    conf (optional) : str
        This should be a dictionary with necessary
        configuration parameters. Its default value
        is an empty dictionary {}.

    """
    future_backends = [
        "dask"
    ]

    global current_backend

    if backend_name in future_backends:
        msg = " This backend environment will be considered in the future !"
        raise NotImplementedError(msg)
    elif backend_name == "local":
        current_backend = Local(conf)
    elif backend_name == "spark":
        from backend.Spark import Spark
        current_backend = Spark(conf)
    else:
        msg = " Incorrect backend environment \"{}\"".format(backend_name)
        raise Exception(msg)


def include(includes_list):
    """
    Includes a list of C++ headers to be declared before execution. Each
    header is also declared on the current running session.

    parameters
    ----------
    includes_list : list or str
        This list should consist of all necessary C++ headers as strings.

    """
    global current_backend, includes

    if isinstance(includes_list, str):
        # Convert to list if this is a string
        includes_list = [includes_list]

    includes.extend(includes_list)
    Utils.declare_headers(includes_list)


def initialize(fun, *args, **kwargs):
    """
    Set a function that will be executed as a first step on every backend before
    any other operation. This method also executes the function on the current
    user environment so changes are visible on the running session.

    This allows users to inject and execute custom code on the worker
    environment without being part of the RDataFrame computational graph.

    Parameters
    ----------
    fun : function
        Function to be executed.

    *args
        Variable length argument list used to execute the function.

    **kwargs
        Keyword arguments used to execute the function.

    """
    Backend.register_initialization(fun, *args, **kwargs)
