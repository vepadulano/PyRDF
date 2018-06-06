from PyRDF import *
import unittest

## Tests for Node class
class OperationReadTest(unittest.TestCase):
    def test_attr_read(self):       
        node = Node(None, None)
        func = node.Define
        self.assertEqual(node._cur_attr, "Define")

    def test_args_read(self):
        node = Node(None, None)
        newNode = node.Define(1, "b", a="1", b=2)
        self.assertEqual(newNode.operation.args, (1, "b"))

    def test_kwargs_read(self):
        node = Node(None, None)
        newNode = node.Define(1, "b", a="1", b=2)
        self.assertEqual(newNode.operation.kwargs, {"a":"1", "b":2})

class NodeReturnTest(unittest.TestCase):
    def test_proxy_return(self):
        node = Node(None, None)
        newNode = node.Count()
        self.assertIsInstance(newNode, Proxy)

    def test_transformation_return(self):
        node = Node(None, None)
        newNode = node.Define(1)
        self.assertIsInstance(newNode, Node)

class DfsTest(unittest.TestCase):
    class Temp(object):
        def __init__(self):
            self.ord_list = []

        def Define(self):
            self.ord_list.append(1)
            return self

        def Filter(self):
            self.ord_list.append(2)
            return self

        def Count(self):
            self.ord_list.append(3)
            return self

    @classmethod
    def traverse(cls, node, prev_object= None):
        """
        Do a depth-first traversal of the graph from the root

        """
        if not node.operation:
            prev_object = DfsTest.Temp()
            node.value = prev_object
            node.graph_prune()

        else:
            ## Execution of the node
            op = getattr(prev_object, node.operation.name)
            node.value = op(*node.operation.args, **node.operation.kwargs)

        for n in node.next_nodes:
            DfsTest.traverse(n, node.value)

        return prev_object.ord_list


    def test_dfs_graph_with_pruning_actions(self):

        node = Node(None, None)

        n1 = node.Define()
        n2 = node.Filter()
        n3 = n2.Filter()
        n4 = n3.Count()
        n5 = n1.Count()
        n6 = node.Filter()

        n5 = n1.Filter()

        obtained_order = DfsTest.traverse(node = node._get_head())

        reqd_order = [1, 2, 2, 2, 3, 2]

        self.assertEqual(obtained_order, reqd_order)

    def test_dfs_graph_with_pruning_transformations(self):

        node = Node(None, None)

        n1 = node.Define()
        n2 = node.Filter()
        n3 = n2.Filter()
        n4 = n3.Count()
        n5 = n1.Filter()
        n6 = node.Filter()

        n5 = n1.Count()

        obtained_order = DfsTest.traverse(node = node._get_head())

        reqd_order = [1, 3, 2, 2, 3, 2]

        self.assertEqual(obtained_order, reqd_order)

    def test_dfs_graph_without_pruning(self):

        node = Node(None, None)

        n1 = node.Define()
        n2 = node.Filter()
        n3 = n2.Filter()
        n4 = n3.Count()
        n5 = n1.Count()
        n6 = node.Filter()

        obtained_order = DfsTest.traverse(node = node._get_head())

        reqd_order = [1, 3, 2, 2, 3, 2]

        self.assertEqual(obtained_order, reqd_order)
