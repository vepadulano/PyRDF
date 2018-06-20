from __future__ import print_function
from .Node import Node
import ROOT

class RDataFrame(Node):
    
    """
    The Python equivalent of ROOT C++'s
    RDataFrame class.

    Attributes
    ----------
    args
        A list of arguments that were provided to
        construct the RDataFrame object.

    Supported constructor arguments
    -------------------------------
    - number of entries (int)
        * For eg., RDataFrame(64)
    - treename (str), fileslist (str or list of str)
        * For eg., RDataFrame("myTree", "file.root") or RDataFrame("myTree", ["file1.root", "file2.root"])
    - treename (str), fileslist (str or list of str or vector of str), default branches (list of str or vector of str)
        * For eg., RDataFrame("myTree", ["file1.root", "file2.root"], ["branch1", "branch2"])

    Raises
    ------
    RDataFrameException
        An exception raised when input arguments to
        the RDataFrame constructor are incorrect.

    """

    def __init__(self, *args):
        """
        Creates a new RDataFrame instance for
        the given arguments.

        Parameters
        ----------
        *args
            Variable length argument list to construct
            the RDataFrame object.

        """

        super(RDataFrame, self).__init__(None, None)

        args = list(args) # Make args mutable
        num_params = len(args)

        for i in range(num_params):
            # Convert Python list to ROOT CPP vector
            if isinstance(args[i], list):
                args[i] = self._get_vector_from_list(args[i])

        try:
            ROOT.ROOT.RDataFrame(*args) # Check if the args are correct
        except TypeError as e:
            rdf_exception = RDataFrameException(e, "Error creating the RDataFrame !")
            rdf_exception.__cause__ = None
            # The above line is to supress the traceback of error 'e'
            raise rdf_exception

        self.args = args


    def _get_vector_from_list(self, arg):
        """
        Converts a python list of strings
        to a vector.

        """
        reqd_vec = ROOT.std.vector('string')()

        for elem in arg:
            reqd_vec.push_back(elem)

        return reqd_vec


class RDataFrameException(Exception):
    """
    A special type of Exception
    that shows up for incorrect
    arguments to RDataFrame.

    """
    def __init__(self, exception, msg):
        """
        Creates a new `RDataFrameException`.

        Parameters
        ----------
        exception
            An exception of type `Exception` or
            any child class of `Exception`.

        msg : str
            Message to be printed while raising exception

        """
        super(RDataFrameException, self).__init__(exception)
        print(msg)
