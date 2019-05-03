from PyRDF.RDataFrame import RDataFrame  # noqa
from PyRDF.RDataFrame import RDataFrameException  # noqa
from PyRDF.CallableGenerator import CallableGenerator  # noqa
from PyRDF.backend.Local import Local
from PyRDF.backend.Backend import Backend
from PyRDF.backend.Utils import Utils
import os

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
        from PyRDF.backend.Spark import Spark
        current_backend = Spark(conf)
    else:
        msg = " Incorrect backend environment \"{}\"".format(backend_name)
        raise Exception(msg)


def include_headers(headers_paths):
    """
    Includes a list of C++ headers to be declared before execution. Each
    header is also declared on the current running session.

    Parameters
    ----------
    headers_paths : str or iterable
        A string or an iterable (such as a list) containing the paths to all
        necessary C++ headers as strings. This function accepts both paths to
        the headers themselves and paths to directories containing the headers.
    """
    global current_backend, includes
    headers_to_include = []

    def get_paths_list(path_string):
        if os.path.isdir(path_string):
            # Create a list with all the headers in the directory
            paths_list = [
                os.path.join(rootpath, filename)
                for rootpath, dirs, filenames
                in os.walk(path_string)
                for filename
                in filenames
            ]
            return paths_list
        elif os.path.isfile(path_string):
            # Convert to list if this is a string
            return [path_string]

    if isinstance(headers_paths, str):
        headers_to_include = get_paths_list(headers_paths)
    else:
        for path_string in headers_paths:
            headers_to_include.extend(get_paths_list(path_string))

    includes.extend(headers_to_include)
    # Converting to set to remove duplicate headers if any.
    # Then converting back to list to pass it to declare_headers()
    headers_to_include = list(set(headers_to_include))

    # If not on the local backend, distribute files to executors
    if not isinstance(current_backend, Local):
        current_backend.distribute_files(headers_to_include)

    Utils.declare_headers(headers_to_include)


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
