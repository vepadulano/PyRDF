from __future__ import print_function

import logging

import ROOT

from PyRDF import Error
from PyRDF.Operation import Operation

logger = logging.getLogger(__name__)


class Node(object):
    """
    A Class that represents a node in RDataFrame operations graph. A Node
    houses an operation and has references to children nodes.
    For details on the types of operations supported, try :

    Example::

        import PyRDF
        PyRDF.use(...) # Choose your backend
        print(PyRDF.current_backend.supported_operations)

    Attributes:
        get_head (function): A lambda function that returns the head node of
            the current graph.

        operation: The operation that this Node represents.
            This could be :obj:`None`.

        children (list): A list of :obj:`PyRDF.Node` objects which represent
            the children nodes connected to the current node.

        _new_op_name (str): The name of the new incoming operation of the next
            child, which is the last child node among the current node's
            children.

        value: The computed value after executing the operation in the current
            node for a particular PyRDF graph. This is permanently :obj:`None`
            for transformation nodes and the action nodes get a
            :obj:`ROOT.RResultPtr` after event-loop execution.

        pyroot_node: Reference to the PyROOT object that implements the
            functionality of this node on the cpp side.

        has_user_references (bool): A flag to check whether the node has
            direct user references, that is if it is assigned to a variable.
            Default value is :obj:`True`, turns to :obj:`False` if the proxy
            that wraps the node gets garbage collected by Python.
    """

    def __init__(self, get_head, operation, *args):
        """
        Creates a new node based on the operation passed as argument.

        Args:
            get_head (function): A lambda function that returns the head node
                of the current graph. This value could be `None`.

            operation (PyRDF.Operation.Operation): The operation that this Node
                represents. This could be :obj:`None`.
        """
        if get_head is None:
            # Function to get 'head' Node
            self.get_head = lambda: self
        else:
            self.get_head = get_head

        self.operation = operation
        self.children = []
        self._new_op_name = ""
        self.value = None
        self.pyroot_node = None
        self.has_user_references = True

    def __getstate__(self):
        """
        Converts the state of the current node
        to a Python dictionary.

        Returns:
            dictionary: A dictionary that stores all instance variables
            that represent the current PyRDF node.

        """
        state_dict = {'children': self.children}
        if self.operation:
            state_dict['operation_name'] = self.operation.name
            state_dict['operation_args'] = self.operation.args
            state_dict['operation_kwargs'] = self.operation.kwargs

        return state_dict

    def __setstate__(self, state):
        """
        Retrieves the state dictionary of the current
        node and sets the instance variables.

        Args:
            state (dict): This is the state dictionary that needs to be
                converted to a `Node` object.

        """
        self.children = state['children']
        if state.get('operation_name'):
            self.operation = Operation(state['operation_name'],
                                       *state['operation_args'],
                                       **state["operation_kwargs"])
        else:
            self.operation = None

    def is_prunable(self):
        """
        Checks whether the current node can be pruned from the computational
        graph.

        Returns:
            bool: True if the node has no children and no user references or
            its value has already been computed, False otherwise.
        """
        if not self.children:
            # Every pruning condition is written on a separate line
            if not self.has_user_references or \
               (self.operation and self.operation.is_action() and self.value):

                # ***** Condition 1 *****
                # If the node is wrapped by a proxy which is not directly
                # assigned to a variable, then it will be flagged for pruning

                # ***** Condition 2 *****
                # If the current node's value was already
                # computed, it should get pruned only if it's
                # an Action node.

                # Logger debug statements
                logger.debug("{} node can be pruned".format(
                    self.operation.name
                ))

                return True

        # Logger debug statements
        if self.operation:  # Node has an operation
            logger.debug("{} node shouldn't be pruned".format(
                self.operation.name
            ))
        else:  # Node is the RDataFrame
            logger.debug("Graph pruning completed")
        return False

    def graph_prune(self):
        """
        Prunes nodes from the current PyRDF graph under certain conditions.
        The current node will be pruned if it has no children and the user
        application does not hold any reference to it. The children of the
        current node will get recursively pruned.

        Returns:
            bool: True if the current node has to be pruned, False otherwise.
        """
        children = []

        # Logger debug statements
        if self.operation:
            logger.debug("Checking {} node for pruning".format(
                self.operation.name
            ))
        else:
            logger.debug("Starting computational graph pruning")

        for n in self.children:
            # Logger debug statement
            # Select children based on pruning condition
            if not n.graph_prune():
                children.append(n)

        self.children = children
        return self.is_prunable()


class HeadNode(Node):
    """
    The Python equivalent of ROOT C++'s
    RDataFrame class.

    Attributes:
        args (list): A list of arguments that were provided to construct
            the RDataFrame object.


    PyRDF's RDataFrame constructor accepts the same arguments as the ROOT's
    RDataFrame constructor (see
    `RDataFrame <https://root.cern/doc/master/classROOT_1_1RDataFrame.html>`_)

    Raises:
        RDataFrameException: An exception raised when input arguments to
            the RDataFrame constructor are incorrect.

    """

    def __init__(self, *args):
        """
        Creates a new RDataFrame instance for the given arguments.

        Args:
            *args (list): Variable length argument list to construct the
                RDataFrame object.
        """
        super(HeadNode, self).__init__(None, None, *args)

        args = list(args)  # Make args mutable

        try:
            ROOT.RDataFrame(*args)  # Check if the args are correct
        except TypeError as e:
            msg = "Error creating the RDataFrame !"
            rdf_exception = Error.RDataFrameException(e, msg)
            rdf_exception.__cause__ = None
            # The above line is to supress the traceback of error 'e'
            raise rdf_exception

        self.args = args

    def get_branches(self):
        """Gets list of default branches if passed by the user."""
        # ROOT Constructor:
        # RDataFrame(TTree& tree, defaultBranches = {})
        if len(self.args) == 2 and isinstance(self.args[0], ROOT.TTree):
            return self.args[1]
        # ROOT Constructors:
        # RDataFrame(treeName, filenameglob, defaultBranches = {})
        # RDataFrame(treename, filenames, defaultBranches = {})
        # RDataFrame(treeName, dirPtr, defaultBranches = {})
        if len(self.args) == 3:
            return self.args[2]

        return None

    def get_num_entries(self):
        """
        Gets the number of entries in the given dataset.

        Returns:
            int: This is the computed number of entries in the input dataset.

        """
        first_arg = self.args[0]
        if isinstance(first_arg, int):
            # If there's only one argument
            # which is an integer, return it.
            return first_arg
        elif isinstance(first_arg, ROOT.TTree):
            # If the argument is a TTree or TChain,
            # get the number of entries from it.
            return first_arg.GetEntries()

        second_arg = self.args[1]

        # Construct a ROOT.TChain object
        chain = ROOT.TChain(first_arg)

        if isinstance(second_arg, str):
            # If the second argument is a string
            chain.Add(second_arg)
        else:
            # If the second argument is a list or vector
            for fname in second_arg:
                chain.Add(str(fname))  # Possible bug in conversion of string

        return chain.GetEntries()

    def get_treename(self):
        """
        Get name of the TTree.

        Returns:
            (str, None): Name of the TTree, or :obj:`None` if there is no tree.

        """
        first_arg = self.args[0]
        if isinstance(first_arg, ROOT.TChain):
            # Get name from a given TChain
            return first_arg.GetName()
        elif isinstance(first_arg, ROOT.TTree):
            # Get name directly from the TTree
            return first_arg.GetUserInfo().At(0).GetName()
        elif isinstance(first_arg, str):
            # First argument was the name of the tree
            return first_arg
        # RDataFrame may have been created without any TTree or TChain
        return None

    def get_tree(self):
        """
        Get ROOT.TTree instance used as an argument to PyRDF.RDataFrame()

        Returns:
            (ROOT.TTree, None): instance of the tree used to instantiate the
            RDataFrame, or `None` if another object was used. ROOT.Tchain
            inherits from ROOT.TTree so that can be the return value as well.
        """
        first_arg = self.args[0]
        if isinstance(first_arg, ROOT.TTree):
            return first_arg

        return None

    def get_inputfiles(self):
        """
        Get list of input files.

        This list can be extracted from a given TChain or from the list of
        arguments.

        Returns:
            (str, list, None): Name of a single file, list of files (both may
            contain globbing characters), or None if there are no input files.

        """
        first_arg = self.args[0]
        if isinstance(first_arg, ROOT.TChain):
            # Extract file names from a given TChain
            chain = first_arg
            return [chainElem.GetTitle()
                    for chainElem in chain.GetListOfFiles()]
        if len(self.args) > 1:
            second_arg = self.args[1]
            if isinstance(second_arg, (str, ROOT.std.vector("string"), list)):
                # Get file(s) from second argument
                # (may contain globbing characters)
                return second_arg
        # RDataFrame may have been created with no input files
        return None
