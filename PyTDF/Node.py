from __future__ import print_function
from .Operation import Operation
from .Proxy import Proxy
import sys, gc

class Node(object):
    """
    A Class that represents
    a graph Node

    """
    def __init__(self, _get_head, operation, tdf_object=None):
        
        if _get_head is None:
            self._get_head = lambda : self
        else:
            self._get_head = _get_head
        
        self.operation = operation
        self.next_nodes = []
        self._cur_attr = ""
        self.value = None

    def __getattr__(self, attr):
        self._cur_attr = attr
        return self._call_handler

    def _call_handler(self, *args, **kwargs):

        op = Operation(self._cur_attr, *args, **kwargs)
        newNode = Node(operation=op, _get_head=self._get_head)
        self.next_nodes.append(newNode)

        if op.op_type == "a":
            return Proxy(newNode)

        return newNode
    
    def _dfs(self, node, prev_object= None):
        """
        Do a depth-first traversal of the graph from the root

        """
        
        if not node.operation:
            prev_object = node._tdf
            self._graph_prune()

        else:
            ## Execution of the node
            op = getattr(prev_object, node.operation.name)
            node.value = op(*node.operation.args, **node.operation.kwargs)
        
        for n in node.next_nodes:
            self._dfs(n, node.value)

    
    def _graph_prune_util(self, node):

        children = []

        for n in node.next_nodes:
            if self._graph_prune_util(n):
                children.append(n)
        node.next_nodes = children

        if len(node.next_nodes) == 0 and (len(gc.get_referrers(node)) <= 3):
            return False

        return True

    
    def _graph_prune(self):
        self._graph_prune_util(self._get_head())
