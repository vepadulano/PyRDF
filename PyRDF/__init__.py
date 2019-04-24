from PyRDF.RDataFrame import RDataFrame  # noqa
from PyRDF.RDataFrame import RDataFrameException  # noqa
from PyRDF.CallableGenerator import CallableGenerator  # noqa
from PyRDF.backend.Local import Local
from PyRDF.backend.Backend import Backend
from PyRDF.backend.Utils import Utils
import os
from os import path

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


def include(includes_list):
    """
    Includes a list of C++ headers to be declared before execution. Each
    header is also declared on the current running session.

    parameters
    ----------
    includes_list : list or str
        If it is a list, it should consist of all necessary C++ headers as
        strings. Otherwise the user can input the path to a directory (or a
        single file) with all the headers needed for the analysis (as a
        string).

    """
    global current_backend, includes

    if isinstance(includes_list, str):
        if path.isdir(includes_list):
            # create a list with all the headers in the directory
            includes_list = [
                path.join(rootpath, filename)
                for rootpath, dirs, filenames
                in os.walk(includes_list)
                for filename
                in filenames
            ]
        elif path.isfile(includes_list):
            # Convert to list if this is a string
            includes_list = [includes_list]

    includes.extend(includes_list)

    # if not on the local backend, distribute files to executors
    if not isinstance(current_backend, Local):
        current_backend.distribute_files(includes_list)

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
