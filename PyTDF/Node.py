from __future__ import print_function
from .Operation import Operation
from .Proxy import Proxy
import sys, gc

class Node(object):
    """
    A Class that represents
    a graph Node

    """
    def __init__(self, _get_head, operation):
        
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

        if op.op_type == op.Types.ACTION:
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

    
    def _graph_prune(self):

        children = []

        for n in self.next_nodes:
            if n._graph_prune():
                children.append(n)
        self.next_nodes = children

        if not self.next_nodes and (len(gc.get_referrers(self)) <= 3):
            
            ### The 3 referrers to the current node would be :
            ### - The current function (_graph_prune())
            ### - An internal reference (which every Python object has)
            ### - The current node's parent node
            ###
            ### [If the user had a variable reference to the current node, 
            ### the value would be at least 4. Hence nodes with less than 
            ### 4 references will have to be removed ]

            ### NOTE :- sys.getrefcount(node) gives a way higher value and hence
            ### doesn't work in this case
            
            return False

        return True
