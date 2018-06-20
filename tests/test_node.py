from PyRDF import *
import unittest

class OperationReadTest(unittest.TestCase):
    """
    A series of test cases to check that
    all new operations are created properly
    inside a new node.

    """

    def test_attr_read(self):
        """
        Test case to check that
        function names are read
        accurately.

        """

        node = Node(None, None)
        func = node.Define
        self.assertEqual(node._cur_attr, "Define")

    def test_args_read(self):
        """
        Test case to check that
        arguments (unnamed) are
        read accurately.

        """

        node = Node(None, None)
        newNode = node.Define(1, "b", a="1", b=2)
        self.assertEqual(newNode.operation.args, (1, "b"))

    def test_kwargs_read(self):
        """
        Test case to check that
        named arguments are
        read accurately.

        """

        node = Node(None, None)
        newNode = node.Define(1, "b", a="1", b=2)
        self.assertEqual(newNode.operation.kwargs, {"a":"1", "b":2})

class NodeReturnTest(unittest.TestCase):
    """
    A series of test cases to check that
    right objects are returned for a node
    (Proxy or Node).

    """

    def test_proxy_return(self):
        """
        Test case to check that
        Proxy objects are returned
        for action nodes.

        """

        node = Node(None, None)
        newNode = node.Count()
        self.assertIsInstance(newNode, Proxy)

    def test_transformation_return(self):
        """
        Test case to check that
        Node objects are returned
        for transformation nodes.

        """

        node = Node(None, None)
        newNode = node.Define(1)
        self.assertIsInstance(newNode, Node)

class DfsTest(unittest.TestCase):
    """
    A series of test cases to check
    that the nodes are traversed in
    the right order for various
    combination of operations.

    """

    class Temp(object):
        """
        A Class for mocking RDF CPP object.

        """

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
        Method to do a depth-first traversal of
        the graph from the root.

        """
        if not node.operation:
            prev_object = DfsTest.Temp()
            node.value = prev_object
            node.graph_prune()

        else:
            ## Execution of the node
            op = getattr(prev_object, node.operation.name)
            node.value = op(*node.operation.args, **node.operation.kwargs)

        for n in node.children:
            DfsTest.traverse(n, node.value)

        return prev_object.ord_list


    def test_dfs_graph_with_pruning_actions(self):
        """
        Test case to check that action nodes with
        no user references get pruned.

        """

        # Head node
        node = Node(None, None)

        # Graph nodes
        n1 = node.Define()
        n2 = node.Filter()
        n3 = n2.Filter()
        n4 = n3.Count()
        n5 = n1.Count()
        n6 = node.Filter()

        # Action pruning
        n5 = n1.Filter() # n5 was an action node earlier

        obtained_order = DfsTest.traverse(node = node.get_head())

        reqd_order = [1, 2, 2, 2, 3, 2]

        self.assertEqual(obtained_order, reqd_order)

    def test_dfs_graph_with_pruning_transformations(self):
        """
        Test case to check that transformation nodes with
        no children and no user references get pruned.

        """

        # Head node
        node = Node(None, None)

        # Graph nodes
        n1 = node.Define()
        n2 = node.Filter()
        n3 = n2.Filter()
        n4 = n3.Count()
        n5 = n1.Filter()
        n6 = node.Filter()

        # Transformation pruning
        n5 = n1.Count() # n5 was earlier a transformation node

        obtained_order = DfsTest.traverse(node = node.get_head())

        reqd_order = [1, 3, 2, 2, 3, 2]

        self.assertEqual(obtained_order, reqd_order)

    def test_dfs_graph_with_recursive_pruning(self):
        """
        Test case to check that nodes in a PyRDF
        graph with no user references and no children
        get pruned recursively.

        """

        # Head node
        node = Node(None, None)

        # Graph nodes
        n1 = node.Define()
        n2 = node.Filter()
        n3 = n2.Filter()
        n4 = n3.Count()
        n5 = n1.Filter()
        n6 = node.Filter()

        # Remove references from n4 and it's parent nodes
        n4 = n3 = n2 = None

        obtained_order = DfsTest.traverse(node = node.get_head())

        reqd_order = [1, 2, 2]

        self.assertEqual(obtained_order, reqd_order)

    def test_dfs_graph_with_parent_pruning(self):
        """
        Test case to check that parent nodes with
        no user references don't get pruned.

        """

        # Head node
        node = Node(None, None)

        # Graph nodes
        n1 = node.Define()
        n2 = node.Filter()
        n3 = n2.Filter()
        n4 = n3.Count()
        n5 = n1.Filter()
        n6 = node.Filter()

        # Remove references from n2 (which shouldn't affect the graph)
        n2 = None

        obtained_order = DfsTest.traverse(node = node.get_head())

        reqd_order = [1, 2, 2, 2, 3, 2]
        # Removing references from n2 will not prune any node
        # because n2 still has children

        self.assertEqual(obtained_order, reqd_order)

    def test_dfs_graph_without_pruning(self):
        """
        Test case to check that node pruning does not
        occur if every node either has children or some
        user references.

        """

        # Head node
        node = Node(None, None)

        # Graph nodes
        n1 = node.Define()
        n2 = node.Filter()
        n3 = n2.Filter()
        n4 = n3.Count()
        n5 = n1.Count()
        n6 = node.Filter()

        obtained_order = DfsTest.traverse(node = node.get_head())

        reqd_order = [1, 3, 2, 2, 3, 2]

        self.assertEqual(obtained_order, reqd_order)
