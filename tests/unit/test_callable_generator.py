from PyRDF import *
import unittest

class CallableGeneratorTest(unittest.TestCase):
    class Temp(object):
        """
        A Class for mocking RDF
        CPP object.

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

    def test_mapper_from_graph(self):
        """
        A simple test case to check
        the working of mapper.

        """

        # A mock RDF object
        t = CallableGeneratorTest.Temp()

        # Head node
        node = Node(None, None)

        # Set of operations to build the graph
        n1 = node.Define()
        n2 = node.Filter().Filter()
        n4 = n2.Count()
        n5 = n1.Count()
        n6 = node.Filter()

        # Generate and execute the mapper
        generator = CallableGenerator(node)
        mapper_func = generator.get_callable()
        values = mapper_func(t)
        nodes = generator.get_action_nodes()

        reqd_order = [1, 3, 2, 2, 3, 2]

        # Assertions
        self.assertEqual(t.ord_list, reqd_order)
        self.assertListEqual(nodes, [n5.action_node, n4.action_node])
        self.assertListEqual(values, [t, t])

    def test_mapper_with_pruning(self):
        """
        A test case to check that the
        mapper works even in the case
        of pruning.

        """

        # A mock RDF object
        t = CallableGeneratorTest.Temp()

        # Head node
        node = Node(None, None)

        # Set of operations to build the graph
        n1 = node.Define()
        n2 = node.Filter().Filter()
        n4 = n2.Count()
        n5 = n1.Count()
        n6 = node.Filter()

        n5 = n1.Filter() # Reason for pruning (change of reference)

        # Generate and execute the mapper
        generator = CallableGenerator(node)
        mapper_func = generator.get_callable()
        values = mapper_func(t)
        nodes = generator.get_action_nodes()

        reqd_order = [1, 2, 2, 2, 3, 2]

        # Assertions
        self.assertEqual(t.ord_list, reqd_order)
        self.assertListEqual(nodes, [n4.action_node])
        self.assertListEqual(values, [t])
