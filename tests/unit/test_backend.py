import PyRDF, unittest, ROOT
from PyRDF import Proxy, Local, Dist

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

    def test_spark_select(self):
        """
        Test to check if 'spark'
        environment gets set correctly.

        """

        PyRDF.use("spark")
        self.assertIsInstance(PyRDF.current_backend, Dist)

    def test_future_env_select(self):
        """
        Test to check if a future environment
        throws a NotImplementedError.

        """

        with self.assertRaises(NotImplementedError):
            PyRDF.use("dask")

class BackendInitTest(unittest.TestCase):
    """
    Tests to ensure that the Backend abstract
    class cannot be instantiated.

    """
    def test_backend_init_error(self):
        """
        Test case to check that any attempt to
        instantiate the `Backend` abstract class
        results in a `TypeError`.

        """
        with self.assertRaises(TypeError):
            PyRDF.backend.Backend.Backend()

    def test_subclass_without_method_error(self):
        """
        Test case to check that creation of a
        subclass without implementing `execute`
        method throws a `TypeError`.

        """
        class TestBackend(PyRDF.backend.Backend.Backend):
            pass

        with self.assertRaises(TypeError):
            TestBackend()

class OperationSupportTest(unittest.TestCase):
    """
    Test cases to ensure that incoming operations are
    classified accurately in local environment.

    """

    def test_action(self):
        """
        Test case to check that action nodes
        are classified accurately.

        """

        backend = Local()
        op = backend.check_supported("Count")

    def test_transformation(self):
        """
        Test case to check that transformation
        nodes are classified accurately.

        """

        backend = Local()
        op = backend.check_supported("Define")

    def test_locally_unsupported_operations(self):
        """
        Test case to check that unsupported operations
        raise an Exception.

        """

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

        backend = Local()
        with self.assertRaises(Exception):
            op = backend.check_supported("random")

    def test_range_operation_single_thread(self):
        """
        Test case to check that 'Range' operation
        works in single-threaded mode and raises an
        Exception in multi-threaded mode.

        """

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
