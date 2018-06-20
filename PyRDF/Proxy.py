from __future__ import print_function
from .CallableGenerator import CallableGenerator

class Proxy(object):
    """
    This class wraps action nodes and instances of Proxy act as
    futures of the result produced by some action. They implement
    a lazy synchronization mechanism, i.e., when they are accessed
    for the first time, they trigger the execution of the whole
    RDataFrame graph.

    Attributes
    ----------
    backend
        A class member to store a backend object
        based on the configuration set by the user.

    action_node
        The action node that the current Proxy
        instance wraps.

    """
    backend = None

    def __init__(self, action_node):
        """
        Creates a new `Proxy` object for a
        given action node.

        Parameters
        ----------
        action_node : PyRDF.Node
            The action node that the current Proxy
            should wrap.

        """
        self.action_node = action_node

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

        self._cur_attr = attr # Stores the name of operation call
        return self._call_handler
    
    def _call_handler(self, *args, **kwargs):
        # Handles an operation call to the current action node
        # and returns result of the current action node.

        if not self.action_node.value: # If event-loop not triggered
            generator = CallableGenerator(self.action_node._get_head())
            Proxy.backend.execute(generator)

        return getattr(self.action_node.value, self._cur_attr)(*args, **kwargs)