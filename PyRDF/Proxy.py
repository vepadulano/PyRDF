from __future__ import print_function
from PyRDF.CallableGenerator import CallableGenerator
from abc import ABCMeta
from PyRDF.Operation import Operation
from PyRDF.Node import Node
# Abstract class declaration
# This ensures compatibility between Python 2 and 3 versions, since in
# Python 2 there is no ABC class
ABC = ABCMeta('ABC', (object,), {})


class Proxy(ABC):
    """
    Abstract class for proxies objects. These objects help to keep track of
    nodes' variable assignment. That is, when a node is no longer assigned
    to a variable by the user, the role of the proxy is to show that. This is
    done via changing the value of the `has_user_references` of the proxied
    node from `True` to `False`.
    """

    def __init__(self, node):
        """
        Creates a new `Proxy` object for a given node.

        Parameters
        ----------
        proxied_node : PyRDF.Node
            The node that the current Proxy should wrap.
        """
        self.proxied_node = node


    def __getattr__(self, attr):
        """
        Intercepts any non-dunder call to the current node
        and dispatches it by means of a call handler.

        Parameters
        ----------
        attr : str
            The name of the operation in the new
            child node.

        Returns
        -------
        function
            A method to handle an operation call to the
            current node.

        """
        # print("Attribute input:", attr, type(attr))
        # print("Cur Attr:", self.proxied_node._cur_attr)
        # print("\n\n")

        # print("Attribute input:", attr, type(attr))
        # print("Cur Attr:", self.proxied_node._cur_attr)
        # print("\n\n")

        # Check if the current call is a dunder method call
        import re
        if re.search("^__[a-z]+__$", attr):
            # Raise an AttributeError for all dunder method calls
            raise AttributeError("Such an attribute is not set ! ")

        from . import current_backend

        if attr in current_backend.supported_operations:
            self.proxied_node._cur_attr = attr  # Stores new operation name
            return self._call_handler
        else:
            try:
                return getattr(self.proxied_node, attr)
            except:
                raise AttributeError("Attribute does not exist")

    def _call_handler(self, *args, **kwargs):
        """
        Handles an operation call to the current node and eturns the new node
        built using the operation call.
        """
        # Create a new `Operation` object for the
        # incoming operation call
        op = Operation(self.proxied_node._cur_attr, *args, **kwargs)

        # Create a new `Node` object to house the operation
        newNode = Node(operation=op, get_head=self.proxied_node.get_head)

        # Add the new node as a child of the current node
        self.proxied_node.children.append(newNode)

        # Return the appropriate proxy object for the node
        if op.is_action():
            return ActionProxy(newNode)
        else:
            return TransformationProxy(newNode)


    def __del__(self):
        """
        This function is called right before the current Proxy gets deleted by
        Python. Its purpose is to show that the wrapped node has no more
        user references, which is one of the conditions for the node to be
        pruned from the computational graph.
        """
        self.proxied_node.has_user_references = False


class ActionProxy(Proxy):
    """
    Instances of ActionProxy act as futures of the result produced
    by some action node. They implement a lazy synchronization
    mechanism, i.e., when they are accessed for the first time,
    they trigger the execution of the whole RDataFrame graph.

    Attributes
    ----------
    backend
        A class member to store a backend object based on the configuration
        set by the user.

    proxied_node
        The action node that the current ActionProxy instance wraps.
    """

    def __getattr__(self, attr):
        """
        Intercepts calls on the result of
        the action node.

        Returns
        -------
        function
            A method to handle an operation call to the
            current action node.

        """
        self._cur_attr = attr  # Stores the name of operation call
        return self._call_handler

    def GetValue(self):
        """
        Returns the result value of the current action
        node if it was executed before, else triggers
        the execution of the entire PyRDF graph before
        returning the value.

        Returns
        -------
        Value of the current action node
            This is the value obtained after executing the
            current action node in the computational graph.
        """
        from . import current_backend
        if not self.proxied_node.value:  # If event-loop not triggered
            generator = CallableGenerator(self.proxied_node.get_head())
            current_backend.execute(generator)

        return self.proxied_node.value

    def _call_handler(self, *args, **kwargs):
        # Handles an operation call to the current action node
        # and returns result of the current action node.
        return getattr(self.GetValue(), self._cur_attr)(*args, **kwargs)


class TransformationProxy(Proxy):
    """
    A proxy object to an instantiated node. Used as a controller of the user
    references to the node itself. When the user deletes reference to a
    node (e.g. assigning the same variable to another operation), the proxy
    object will get destroyed by Python, thus flagging the node to be without
    user references anymore.
    """