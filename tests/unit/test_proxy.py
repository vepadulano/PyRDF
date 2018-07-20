from PyRDF.Proxy import Proxy
from PyRDF.Node import Node
import unittest

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