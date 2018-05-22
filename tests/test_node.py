from PyTDF import *
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

    def test_dfs_order_small(self):
        t = DfsTest.Temp()

        node = Node(None, None)
        node.value = t

        node._dfs(node = node._get_head())

        self.assertEqual(t.ord_list, [])

    def test_dfs_order_big(self):
        t = DfsTest.Temp()

        node = Node(None, None)
        node.value = t
        n1 = node.Define()
        n2 = node.Filter()
        n3 = n2.Filter()
        n4 = n3.Count()
        n5 = n1.Count()
        n6 = node.Filter()

        node._dfs(node = node._get_head())

        reqd_order = [1, 3, 2, 2, 3, 2]

        self.assertEqual(t.ord_list, reqd_order)

    def test_dfs_graph_pruning(self):
        t = DfsTest.Temp()

        node = Node(None, None)
        node.value = t
        n1 = node.Define()
        n2 = node.Filter()
        n3 = n2.Filter()
        n4 = n3.Count()
        n5 = n1.Count()
        n6 = node.Filter()

        n5 = n1.Filter()

        node._dfs(node = node._get_head())

        reqd_order = [1, 2, 2, 2, 3, 2]

        self.assertEqual(t.ord_list, reqd_order)
