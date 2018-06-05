from PyRDF import *
import unittest

class CallableGeneratorTest(unittest.TestCase):
    class Temp(object):
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
        t = CallableGeneratorTest.Temp()

        node = Node(None, None)
        node.value = t
        n1 = node.Define()
        n2 = node.Filter().Filter()
        n4 = n2.Count()
        n5 = n1.Count()
        n6 = node.Filter()

        mp = CallableGenerator(node)
        mapper_func = mp.get_callable()
        actions = mapper_func(t)

        reqd_order = [1, 3, 2, 2, 3, 2]

        self.assertEqual(t.ord_list, reqd_order)
        self.assertEqual(list(actions.keys()), ['ta1', 'ta2'])

    def test_mapper_with_pruning(self):
        t = CallableGeneratorTest.Temp()

        node = Node(None, None)
        node.value = t
        n1 = node.Define()
        n2 = node.Filter().Filter()
        n4 = n2.Count()
        n5 = n1.Count()
        n6 = node.Filter()

        n5 = n1.Filter()

        mp = CallableGenerator(node)
        mapper_func = mp.get_callable()
        actions = mapper_func(t)

        reqd_order = [1, 2, 2, 2, 3, 2]

        self.assertEqual(t.ord_list, reqd_order)
        self.assertEqual(list(actions.keys()), ['ta1'])
