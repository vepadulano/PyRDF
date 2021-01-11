import logging
import multiprocessing
import os
import threading

try:
    import queue
except ImportError:
    import Queue as queue

import PyRDF
import ROOT

logger = logging.getLogger(__name__)


class Utils(object):
    """Class that houses general utility functions."""
    @classmethod
    def extend_include_path(cls, include_path):
        """
        Extends the list of paths in which ROOT looks for headers and
        libraries. Every header directory is added to the internal include
        path of ROOT so the interpreter can find them. Even if the same path
        is added twice, ROOT keeps a collection of unique paths. Find more at
        `TInterpreter<https://root.cern.ch/doc/master/classTInterpreter.html>`_

        Args:
            include_path (str): the path to the directory containing files
                needed for the analysis.
        """
        root_path = "-I{}".format(include_path)
        ROOT.gInterpreter.AddIncludePath(root_path)

        # Retrieve ROOT internal list of include paths and add debug statement
        root_includepath = ROOT.gInterpreter.GetIncludePath()
        logger.debug("ROOT include paths:\n%s", root_includepath)

    @classmethod
    def declare_headers(cls, headers_to_include):
        """
        Declares all required headers using the ROOT's C++ Interpreter.

        Args:
            headers_to_include (list): This list should consist of all
                necessary C++ headers as strings.
        """
        for header in headers_to_include:
            # Retrieve header directory
            header_dir = os.path.dirname(header)
            # Add directory to ROOT's include path
            cls.extend_include_path(header_dir)
            # Create C++ include code
            include_code = "#include \"{}\"\n".format(header)
            try:
                ROOT.gInterpreter.Declare(include_code)
            except Exception as e:
                msg = "There was an error in including \"{}\" !".format(header)
                raise e(msg)

    @classmethod
    def declare_shared_libraries(cls, libraries_to_include):
        """
        Declares all required shared libraries using the ROOT's C++
        Interpreter.

        Args:
            libraries_to_include (list): This list should consist of all
                necessary C++ shared libraries as strings.
        """
        for shared_library in libraries_to_include:
            # Get return value for loading the shared library.
            # On succesful load the value will be 0.
            # If the library does not exist or there was an error
            # while loading, the value will be -1
            lib_load_return = ROOT.gSystem.Load(shared_library)
            if lib_load_return == -1:
                if not os.path.exists(shared_library):
                    raise IOError("Shared library does not exist!")
                raise Exception("ROOT couldn't load the shared library!")

    @classmethod
    def RunGraphs(cls, proxies, concurrent_runs=4):
        """
        Trigger the execution of multiple RDataFrame computation graphs on the
        distributed backend in use. If the backend doesn't support multiple job
        submissions concurrently, the distributed computation graphs will be
        executed sequentially.

        Args:
            proxies(list): List of action proxies that should be triggered. Only
                actions belonging to different RDataFrame graphs will be
                triggered to avoid useless calls.

            concurrent_runs(int, optional): Number of graphs that will be
                executed concurrently in a distributed backend. Defaults to 4.

        Example::

            # Create 3 different dataframes and book an histogram on each one
            histoproxies = [
                PyRDF.RDataFrame(100)
                    .Define("x", "rdfentry_")
                    .Histo1D(("name", "title", 10, 0, 100), "x")
                for _ in range(4)
            ]

            # Execute the 3 computation graphs
            PyRDF.backend.Utils.RunGraphs(histoproxies)

            # Retrieve all the histograms in one go
            histos = [histoproxy.GetValue() for histoproxy in histoproxies]
        """

        # Get proxies belonging to distinct computation graphs
        uniquegraphs = {proxy.proxied_node.get_head(): proxy
                        for proxy in proxies}.values()

        try:
            PyRDF.current_backend.RunGraphs(uniquegraphs, concurrent_runs)
        except NotImplementedError:
            for proxy in uniquegraphs:
                proxy.GetValue()
