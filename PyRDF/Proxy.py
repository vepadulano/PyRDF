from __future__ import print_function
from PyRDF.CallableGenerator import CallableGenerator
from abc import ABCMeta, abstractmethod
import functools


def trackcalls(func):
    """
    function decorator that tracks if a function has been called.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        wrapper.has_been_called = True
        return func(*args, **kwargs)
    wrapper.has_been_called = False
    return wrapper


ABC = ABCMeta('ABC', (object,), {})


class Proxy(ABC):
    """
    Abstract class for proxies objects. These objects help to keep track of
    nodes' variable assignment. That is, when a node is no longer assigned
    to a variable by the user, the role of the proxy is to flag that node as
    `prunable`. There are two main classes for proxies, depending on the
    operation type of the node they are wrapping:
        - ActionProxy: a proxy wrapping a node that holds an action operation.
        - TransformationProxy: a proxy wrapping a node that holds a
        transformation operation.
    """

    def __init__(self, node):
        """
        Creates a new `Proxy` object for a
        given node.

        Parameters
        ----------
        proxied_node : PyRDF.Node
            The node that the current Proxy
            should wrap.
        """
        self.proxied_node = node
        # self.getstate_called = False

    @abstractmethod
    def __del__(self):
        """
        Proxy has to flag a node as prunable when the user changes
        the variable assigned to it.
        """
        pass


class ActionProxy(Proxy):
    """
    Instances of Proxy act as futures of the result produced
    by some action. They implement a lazy synchronization
    mechanism, i.e., when they are accessed for the first time,
    they trigger the execution of the whole RDataFrame graph.

    Attributes
    ----------
    backend
        A class member to store a backend object
        based on the configuration set by the user.

    node
        The action node that the current Proxy
        instance wraps.

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

    def __del__(self):
        """Deletes current Proxy and flags the wrapped Node for pruning"""
        self.proxied_node.has_user_references = False

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
        if not self.proxied_node.value:  # If event-loop not triggered
            from . import current_backend
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

    def __del__(self):
        """Deletes current Proxy and flags the wrapped Node for pruning"""
        self.proxied_node.has_user_references = False

    def __getattr__(self, attr):
        """
        Intercepts calls on operation or attributes belonging to the proxied
        node.

        Returns either:
        -------
        function
            If the attribute passed by the user is a supported operation, the
            proxy will return a method to handle an operation call to the
            current transformation node.

        node attribute
            If the attribute passed by the user is not an operation, the proxy
            will try to return the corresponding attribute of the proxied node.
        """

        # Check if the parameter `attr` is an operation supported by
        # the backend
        from . import current_backend
        if attr in current_backend.supported_operations:
            # Stores the name of operation call in the node attributes
            self.proxied_node._cur_attr = attr
            return self.proxied_node._call_handler
        else:
            return getattr(self.proxied_node, attr)

    @trackcalls
    def __getstate__(self):
        """
        Function that gets called when a call to pickle.dumps is issued.
        """
        return self.__dict__
