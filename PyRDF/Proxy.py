from __future__ import print_function
from .CallableGenerator import CallableGenerator

class Proxy(object):
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

    action_node
        The action node that the current Proxy
        instance wraps.

    """
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
        if not self.action_node.value: # If event-loop not triggered
            from . import current_backend
            generator = CallableGenerator(self.action_node.get_head())
            current_backend.execute(generator)

        return self.action_node.value

    def _call_handler(self, *args, **kwargs):
        # Handles an operation call to the current action node
        # and returns result of the current action node.

        return getattr(self.GetValue(), self._cur_attr)(*args, **kwargs)