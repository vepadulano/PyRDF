from PyRDF.RDataFrame import RDataFrame  # noqa
from PyRDF.RDataFrame import RDataFrameException  # noqa
from PyRDF.CallableGenerator import CallableGenerator  # noqa
from PyRDF.backend.Local import Local
from PyRDF.backend.Backend import Backend
from PyRDF.backend.Utils import Utils
import os

current_backend = Local()
includes_headers = []  # All headers included in the analysis
includes_shared_libraries = []  # All shared libraries included in the analysis
includes_files = []  # All other generic files included


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


def _get_paths_list_from_string(path_string):
    """Retrieves paths to files (directory or single file) from a string."""
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
    global current_backend, includes_headers
    headers_to_include = []

    if isinstance(headers_paths, str):
        headers_to_include = _get_paths_list_from_string(headers_paths)
    else:
        for path_string in headers_paths:
            headers_to_include.extend(_get_paths_list_from_string(path_string))

    # Converting to set to remove duplicate headers if any.
    # Then converting back to list to pass it to declare_headers()
    headers_to_include = list(set(headers_to_include))

    # If not on the local backend, distribute files to executors
    if not isinstance(current_backend, Local):
        current_backend.distribute_files(headers_to_include)

    Utils.declare_headers(headers_to_include)

    # Finally, add everything to the includes list
    includes_headers.extend(headers_to_include)


def include_shared_libraries(shared_libraries_paths):
    """
    Includes a list of C++ shared libraries to be declared before execution.
    Each library is also declared on the current running session. If any pcm
    file is present in the same folder as the shared libraries, the function
    will try to retrieve them (and distribute them if working on a distributed
    backend). Otherwise the user can input the paths to their own pcm files
    with the `send_pcm_files()` function.

    Parameters
    ----------
    shared_libraries_paths : str or iterable
        A string or an iterable (such as a list) containing the paths to all
        necessary C++ shared libraries as strings. This function accepts both
        paths to the libraries themselves and paths to directories containing
        the libraries.
    """
    global current_backend, includes_shared_libraries
    libraries_to_include = []
    pcm_to_include = []

    def _check_pcm_in_library_path(shared_library_path):
        all_paths = _get_paths_list_from_string(
            shared_library_path
        )

        pcm_paths = [
            filepath
            for filepath in all_paths
            if filepath.split(".")[-1] == "pcm"
        ]

        libraries_path = [
            filepath
            for filepath in all_paths
            if filepath not in pcm_paths
        ]

        return pcm_paths, libraries_path

    if isinstance(shared_libraries_paths, str):
        pcm_to_include, libraries_to_include = _check_pcm_in_library_path(
            shared_libraries_paths
        )
    else:
        for path_string in shared_libraries_paths:
            pcm, libraries = _check_pcm_in_library_path(
                path_string
            )
            libraries_to_include.extend(libraries)
            pcm_to_include.extend(pcm)

    # Converting to set to remove duplicate headers if any.
    # Then converting back to list to pass it to declare_shared_libraries()
    libraries_to_include = list(set(libraries_to_include))
    pcm_to_include = list(set(pcm_to_include))

    # If not on the local backend, distribute files to executors
    if not isinstance(current_backend, Local):
        current_backend.distribute_files(libraries_to_include)
        current_backend.distribute_files(pcm_to_include)

    Utils.declare_shared_libraries(libraries_to_include)

    # Finally, add everything to the includes list
    includes_shared_libraries.extend(includes_shared_libraries)


def send_generic_files(files_paths):
    """
    Sends to the workers the generic files needed by the user.

    Parameters
    ----------
    files_paths : str or iterable
        Paths to the files to be sent to the distributed workers.
    """
    global current_backend, includes_files
    files_to_include = []

    if isinstance(files_paths, str):
        files_to_include = _get_paths_list_from_string(files_paths)
    else:
        for path_string in files_paths:
            files_to_include.extend(_get_paths_list_from_string(path_string))

    # Converting to set to remove duplicate headers if any.
    # Then converting back to list to pass it to declare_headers()
    files_to_include = list(set(files_to_include))

    # If not on the local backend, distribute files to executors
    if not isinstance(current_backend, Local):
        current_backend.distribute_files(files_to_include)


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
