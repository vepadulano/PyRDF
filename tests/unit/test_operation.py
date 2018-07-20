from PyRDF.Operation import Operation
import unittest

class ClassifyTest(unittest.TestCase):
    """
    Test cases to ensure that incoming
    operations are classified accurately.

    """

    def test_action(self):
        """
        Test case to check that action nodes
        are classified accurately.

        """

        op = Operation("Count")
        self.assertEqual(op.op_type, Operation.Types.ACTION)

    def test_instant_action(self):
        """
        Test case to check that instant
        actions are classified accurately.

        """
        op = Operation("Snapshot")
        self.assertEqual(op.op_type, Operation.Types.INSTANT_ACTION)

    def test_transformation(self):
        """
        Test case to check that transformation
        nodes are classified accurately.

        """

        op = Operation("Define", "c1")
        self.assertEqual(op.op_type, Operation.Types.TRANSFORMATION)

    def test_none(self):
        """
        Test case to check that incorrect operations
        raise an Exception.

        """

        with self.assertRaises(Exception):
            op = Operation("random")

class ArgsTest(unittest.TestCase):
    """
    Test cases to ensure that arguments
    and named arguments are set accurately.

    """

    def test_without_kwargs(self):
        """
        Test case to check accuracy of
        all arguments with no named
        arguments as input.

        """

        op = Operation("Define", 1, "b")
        self.assertEqual(op.args, (1, "b"))
        self.assertEqual(op.kwargs, {})

    def test_without_args(self):
        """
        Test case to check accuracy of
        all arguments with no unnamed
        arguments as input.

        """

        op = Operation("Define", a=1, b="b")
        self.assertEqual(op.args, ())
        self.assertEqual(op.kwargs, {"a":1, "b":"b"})

    def test_with_args_and_kwargs(self):
        """
        Test case to check accuracy of
        all arguments with both named and
        unnamed arguments in the input.

        """

        op = Operation("Define", 2, "p", a=1, b="b")
        self.assertEqual(op.args, (2, "p"))
        self.assertEqual(op.kwargs, {"a":1, "b":"b"})

    def test_without_args_and_kwargs(self):
        """
        Test case to check accuracy of
        all arguments with no arguments
        at all in the input.

        """

        op = Operation("Define")
        self.assertEqual(op.args, ())
        self.assertEqual(op.kwargs, {})
