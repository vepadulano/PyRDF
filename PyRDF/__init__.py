from .RDataFrame import RDataFrame, RDataFrameException
from .CallableGenerator import CallableGenerator
from backend.Local import Local
from backend.Backend import Backend

current_backend = Local()
includes = []

def use(backend_name, conf = {}):
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
        raise NotImplementedError(" This backend environment will be considered in the future !")
    elif backend_name == "local":
        current_backend = Local(conf)
    elif backend_name == "spark":
        from backend.Spark import Spark
        current_backend = Spark(conf)
    else:
        raise Exception(" Incorrect backend environment \"{}\"".format(backend))

def include(includes_list):
    """
    Includes a list of C++ headers to be
    declared before execution.

    parameters
    ----------
    includes_list : list or str
        This list should consist of all necessary C++
        headers as strings.

    """
    global current_backend, includes

    if isinstance(includes_list, str):
        # Convert to list if this is a string
        includes_list = [includes_list]

    includes.extend(includes_list)

def initialize(fun, *args, **kwargs):
    """
    Set a function that will be executed as a first step on every backend before
    any other operation.

    This allows users to inject and execute custom code on the worker environment
    without being part of the RDataFrame computational graph.

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
