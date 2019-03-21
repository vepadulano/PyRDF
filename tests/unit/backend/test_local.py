import PyRDF, unittest, ROOT
from PyRDF.backend.Local import Local
from PyRDF.Proxy import Proxy

class SelectionTest(unittest.TestCase):
    """
    A series of tests to check the
    accuracy of 'PyRDF.use' method.

    """

    def test_local_select(self):
        """
        Test to check if 'local'
        environment gets set correctly.

        """

        PyRDF.use("local")
        self.assertIsInstance(PyRDF.current_backend, Local)

class OperationSupportTest(unittest.TestCase):
    """
    Test cases to ensure that incoming operations are
    classified accurately in local environments.

    """

    def test_action(self):
        """
        Test case to check that action nodes
        are classified accurately.
        """

        # Check in local env
        backend = Local()
        op = backend.check_supported("Count")

    def test_transformation(self):
        """
        Test case to check that transformation
        nodes are classified accurately.
        """

        # Check in local env
        backend = Local()
        op = backend.check_supported("Define")

    def test_unsupported_operations(self):
        """
        Test case to check that unsupported operations
        raise an Exception.
        """

        # Check in local env
        backend = Local()
        with self.assertRaises(Exception):
            op = backend.check_supported("Take")

        with self.assertRaises(Exception):
            op = backend.check_supported("Snapshot")

        with self.assertRaises(Exception):
            op = backend.check_supported("Foreach")

    def test_none(self):
        """
        Test case to check that incorrect operations
        raise an Exception.
        """

        # Check in local env
        backend = Local()
        with self.assertRaises(Exception):
            op = backend.check_supported("random")

    def test_range_operation_single_thread(self):
        """
        Test case to check that 'Range' operation
        works in single-threaded mode and raises an
        Exception in multi-threaded mode.
        """

        # Check in local env
        backend = Local()
        backend.check_supported("Range")

    def test_range_operation_multi_thread(self):
        """
        Test case to check that 'Range' operation
        raises an Exception in multi-threaded mode.
        """

        ROOT.ROOT.EnableImplicitMT()
        backend = Local()
        with self.assertRaises(Exception):
            op = backend.check_supported("Range")

        ROOT.ROOT.DisableImplicitMT()

class BroadcastInitializationTest(unittest.TestCase):
    """
    Check initialization method in the Local backend
    """
    def test_initialization_method(self):
       # Define a method in the ROOT interprete called getValue
       # that returns the value defined by the user
       def init(value):
           cpp_code = '''auto getUserValue = [](){return %s ;};''' % value
           ROOT.gInterpreter.Declare(cpp_code)

       PyRDF.broadcastInitialization(init, 123)
       backend = Local()
       df = PyRDF.RDataFrame(1)
       s = df.Define("userValue", "getUserValue()").Sum("userValue")
       self.assertEqual(s.GetValue(), 123)
