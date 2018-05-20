from PyTDF import *
import unittest

class ClassifyTest(unittest.TestCase):
	def test_action(self):
		op = Operation("Count")
		self.assertEqual(op.op_type, "a")

	def test_transformation(self):
		op = Operation("Define", "c1")
		self.assertEqual(op.op_type, "t")

	def test_none(self):
		op = Operation("random")
		self.assertIs(op.op_type, None)


class ArgsTest(unittest.TestCase):
	def test_without_kwargs(self):
		op = Operation("Define", 1, "b")
		self.assertEqual(op.args, (1, "b"))
		self.assertEqual(op.kwargs, {})

	def test_without_args(self):
		op = Operation("Define", a=1, b="b")
		self.assertEqual(op.args, ())
		self.assertEqual(op.kwargs, {"a":1, "b":"b"})

	def test_with_args_and_kwargs(self):
		op = Operation("Define", 2, "p", a=1, b="b")
		self.assertEqual(op.args, (2, "p"))
		self.assertEqual(op.kwargs, {"a":1, "b":"b"})

	def test_without_args_and_kwargs(self):
		op = Operation("Define")
		self.assertEqual(op.args, ())
		self.assertEqual(op.kwargs, {})
