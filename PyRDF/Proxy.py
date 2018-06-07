from __future__ import print_function
from .CallableGenerator import CallableGenerator

class Proxy(object):
    """
    Proxy provides an interface 
    between the user and the library
    to exchange the results of action
    nodes

    """

    backend = None

    def __init__(self, action_node):

        self.action_node = action_node

    def __getattr__(self, attr):

        self._cur_attr = attr
        return self._call_handler
    
    def _call_handler(self, *args, **kwargs):

        if not self.action_node.value:
            generator = CallableGenerator(self.action_node._get_head())
            Proxy.backend.execute(generator)

        return getattr(self.action_node.value, self._cur_attr)(*args, **kwargs)