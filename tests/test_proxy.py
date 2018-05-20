from PyTDF import *
import unittest

class AttrReadTest(unittest.TestCase):

	class Temp(object):
		def val(self, arg):
			return arg+123

	def test_attr_simple(self):
		# Write a new class again
		node = Node(None, None)
		proxy = Proxy(node)
		func = proxy.attr

		self.assertEqual(proxy._cur_attr, "attr")

	def test_return_value(self):

		t = AttrReadTest.Temp()
		node = Node(None, None)
		node.value = t
		proxy = Proxy(node)

		self.assertEqual(proxy.val(21), 144)