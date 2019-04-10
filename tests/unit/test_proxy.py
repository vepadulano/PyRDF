from PyRDF.Proxy import Proxy, ActionProxy, TransformationProxy
from PyRDF.Node import Node
from PyRDF.backend.Backend import Backend
from PyRDF import RDataFrame
import unittest
import PyRDF
from PyRDF import current_backend


class ProxyInitTest(unittest.TestCase):
    """Proxy abstract class cannot be instantiated."""

    def test_proxy_init_error(self):
        """
        Any attempt to instantiate the `Proxy` abstract class results in
        a `TypeError`.

        """
        with self.assertRaises(TypeError):
            Proxy()

    def test_subclass_without_method_error(self):
        """
        Creation of a subclass without implementing `delete`
        method throws a `TypeError`.

        """
        class TestProxy(Proxy):
            pass

        with self.assertRaises(TypeError):
            TestProxy()


class TypeReturnTest(unittest.TestCase):
    """Tests that right types are returned"""

    def test_type_return_transformation(self):
        """
        TransformationProxy object is of type `PyRDF.TransformationProxy` and
        wraps a node object.
        """
        node = Node(None, None)
        proxy = TransformationProxy(node)
        self.assertIsInstance(proxy, TransformationProxy)
        self.assertIsInstance(proxy.proxied_node, Node)

    def test_type_return_action(self):
        """
        ActionProxy object is of type `PyRDF.ActionProxy` and
        wraps a node object.
        """
        node = Node(None, None)
        proxy = ActionProxy(node)
        self.assertIsInstance(proxy, ActionProxy)
        self.assertIsInstance(proxy.proxied_node, Node)


class AttrReadTest(unittest.TestCase):
    """Test Proxy class methods."""

    class Temp(object):
        """A mock action node result class."""

        def val(self, arg):
            """A test method to check function call on the Temp class."""
            return arg + 123  # A simple operation to check

    def test_attr_simple_action(self):
        """ActionProxy object reads the right input attribute."""
        node = Node(None, None)
        proxy = ActionProxy(node)
        func = proxy.attr

        self.assertEqual(proxy._cur_attr, "attr")
        self.assertTrue(callable(func))

    def test_attr_simple_transformation(self):
        """
        TransformationProxy object reads the right input attributes,
        returning the methods and the attributes of the proxied node.
        """
        node = Node(None, None)
        proxy = TransformationProxy(node)

        for attr in current_backend.supported_operations:
            getattr(proxy, attr)
            self.assertEqual(proxy.proxied_node._cur_attr, attr)
            self.assertTrue(callable(proxy.proxied_node._call_handler))

        proxy.attribute
        self.assertEqual(proxy.proxied_node._cur_attr, "attribute")

    def test_proxied_node_has_user_references(self):
        """check that the user reference holds until the proxy lives."""
        node = Node(None, None)
        proxy = TransformationProxy(node)
        self.assertTrue(node.has_user_references)
        proxy = None # noqa: avoid PEP8 F841
        self.assertFalse(node.has_user_references)

    def test_return_value(self):
        """
        Proxy object computes and returns the right output based on the
        function call.

        """
        t = AttrReadTest.Temp()
        node = Node(None, None)
        node.value = t
        proxy = ActionProxy(node)

        self.assertEqual(proxy.val(21), 144)


class GetValueTests(unittest.TestCase):
    """Check 'GetValue' instance method in Proxy."""

    class TestBackend(Backend):
        """
        Test backend to verify the working of 'GetValue' instance method
        in Proxy.

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
        proxy = ActionProxy(node)
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


class PickleTest(unittest.TestCase):
    """Tests to check the pickling an un-pickling of Node instances."""

    def test_proxy_pickle(self):
        """
        Test case to check that TransformationProxy objects cannot
        be unpickled.
        """

        import pickle

        node = Node(None, None)
        proxy = TransformationProxy(node)
        pickle.dumps(proxy)
        self.assertTrue(proxy.__getstate__.has_been_called)
