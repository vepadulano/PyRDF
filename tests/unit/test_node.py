from PyRDF.Node import Node
from PyRDF.Proxy import Proxy
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

    def test_dfs_graph_with_computed_values_pruning(self):
        """
        """

        # Head node
        node = Node(None, None)

        # Graph nodes
        n1 = node.Define()
        n2 = node.Filter()
        n3 = n2.Filter()
        n4 = n3.Count()
        n5 = n1.Filter()
        n6 = n5.Count()
        n7 = node.Filter()

        # This is to make sure action nodes with
        # already computed values are pruned.
        n6.action_node.value = 1
        # This is to make sure that transformation
        # leaf nodes with value (possibly set intentionally)
        # don't get pruned.
        n7.value = 1

        obtained_order = DfsTest.traverse(node = node.get_head())

        # The node 'n6' will be pruned. Hence,
        # there's only one '3' in this list.
        reqd_order = [1, 2, 2, 2, 3, 2]

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

class DunderMethodsTest(unittest.TestCase):
    """
    Test cases to check the response of
    the Node class for various dunder
    method calls.

    """
    def test_get_state(self):
        """
        Test cases to check the working of
        __getstate__ method on Node class.

        """
        node = Node(None, None) # Head node
        n1 = node.Define("a", b="c") # First child node

        # Required dictionaries
        node_dict = {"children":[n1]}
        n1_dict = {
        'operation_name':"Define",
        'operation_args':("a",),
        'operation_kwargs':{"b":"c"},
        'children':[]
        }

        self.assertDictEqual(node.__getstate__(), node_dict)
        self.assertDictEqual(n1.__getstate__(), n1_dict)

    def test_set_state(self):
        """
        Test cases to check the working of
        __setstate__ method on Node class.

        """
        node = Node(None, None) # Head node
        n1 = Node(None, None) # First child node

        # State dictionaries
        node_dict = {"children":[n1]}
        n1_dict = {
        'operation_name':"Define",
        'operation_args':("a",),
        'operation_kwargs':{"b":"c"},
        'children':[]
        }

        # Set node objects with state dicts
        node.__setstate__(node_dict)
        n1.__setstate__(n1_dict)

        self.assertListEqual([node.operation, node.children], [None, node_dict["children"]])
        self.assertListEqual([
            n1.operation.name,
            n1.operation.args,
            n1.operation.kwargs,
            n1.children
            ], [
            n1_dict["operation_name"],
            n1_dict["operation_args"],
            n1_dict["operation_kwargs"],
            n1_dict["children"]
            ])

    def test_other_dunder_methods(self):
        """
        Test cases to check the working of
        other dunder methods on Node class.

        """
        node = Node(None, None)

        # Regular dunder method must not throw an error
        node.__format__('')

        with self.assertRaises(AttributeError):
            node.__random__() # Unknown dunder method

class PickleTest(unittest.TestCase):
    """
    Tests to check the pickling an un-pickling of
    Node instances.

    """
    def assertGraphs(self, current_node1, current_node2):
        """
        Asserts every node in the given graphs starting
        from the given head nodes.

        """
        self.assertNodes(current_node1, current_node2) # Equality between nodes

        for n1, n2 in zip(current_node1.children, current_node2.children):
            self.assertGraphs(n1, n2) # Check the rest of the graphs

    def assertNodes(self, node1, node2):
        """
        Assert equality between two given nodes by checking
        the operation and number of children nodes.

        """
        self.assertEqual(len(node1.children), len(node2.children)) # Check the number of children

        # Assert operations
        if not node1.operation:
            self.assertIsNone(node2.operation)
        else:
            self.assertEqual(node1.operation.name, node2.operation.name)
            self.assertEqual(node1.operation.args, node2.operation.args)
            self.assertEqual(node1.operation.kwargs, node2.operation.kwargs)

    def test_node_pickle(self):
        """
        Test cases to check that nodes can be accurately
        pickled and un-pickled.

        """
        import pickle

        # Node definitions
        node = Node(None, None) # Head node
        n1 = node.Define("a", b="c") # First child node
        n2 = n1.Count()
        n3 = node.Filter("b")
        n4 = n3.Count()

        # Pickled representation of nodes
        pickled_node = pickle.dumps(node)
        pickled_n3 = pickle.dumps(n3)

        # Un-pickled node objects
        unpickled_node = pickle.loads(pickled_node)
        unpickled_n3 = pickle.loads(pickled_n3)

        self.assertIsInstance(unpickled_node, type(node))
        self.assertIsInstance(unpickled_n3, type(n3))
        self.assertGraphs(node, unpickled_node)
        self.assertGraphs(n3, unpickled_n3)
