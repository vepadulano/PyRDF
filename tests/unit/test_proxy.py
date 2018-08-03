from PyRDF.Proxy import Proxy
from PyRDF.Node import Node
from PyRDF.backend.Backend import Backend
from PyRDF import RDataFrame
import unittest, PyRDF

class AttrReadTest(unittest.TestCase):
    """
    Test cases to check working of
    methods in Proxy class.

    """

    class Temp(object):
        """
        A mock action node result class.

        """
        def val(self, arg):
            """
            A test method to check function
            call on the Temp class.

            """
            return arg+123 # A simple operation to check

    def test_attr_simple(self):
        """
        Test case to check that a Proxy
        object reads the right input
        attribute.

        """

        node = Node(None, None)
        proxy = Proxy(node)
        func = proxy.attr

        self.assertEqual(proxy._cur_attr, "attr")

    def test_return_value(self):
        """
        Test case to check that a Proxy
        object computes and returns the
        right output based on the function call.

        """

        t = AttrReadTest.Temp()
        node = Node(None, None)
        node.value = t
        proxy = Proxy(node)

        self.assertEqual(proxy.val(21), 144)

class GetValueTests(unittest.TestCase):
    """
    Test cases to check the working of
    'GetValue' instance method in Proxy.

    """
    class TestBackend(Backend):
        """
        Test backend to verify the working of
        'GetValue' instance method in Proxy.

        """
        def execute(self, generator):
            """
            Test implementation of the execute method
            for 'TestBackend'. This records the head
            node of the input PyRDF graph from the
            generator object.

            """
            self.obtained_head_node = generator.head_node

    def test_get_value_with_existing_value(self):
        """
        Test case to check the working of 'GetValue'
        method in Proxy when the current action node
        already houses a value.

        """
        node = Node(None, None)
        proxy = Proxy(node)
        node.value = 5

        self.assertEqual(proxy.GetValue(), 5)

    def test_get_value_with_value_not_existing(self):
        """
        Test case to check the working of 'GetValue'
        method in Proxy when the current action node
        doesn't contain a value (event-loop hasn't been
        triggered yet).

        """
        PyRDF.current_backend = GetValueTests.TestBackend()

        rdf = RDataFrame(10)
        count = rdf.Count()

        count.GetValue()

        # Ensure that TestBackend's execute method was called
        self.assertIs(PyRDF.current_backend.obtained_head_node, rdf)