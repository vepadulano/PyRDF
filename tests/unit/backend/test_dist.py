import PyRDF, unittest, ROOT
from PyRDF import backend

class DistBackendInitTest(unittest.TestCase):
    """
    Tests to ensure that the Dist abstract
    class cannot be instantiated.

    """
    def test_dist_init_error(self):
        """
        Test case to check that any attempt to
        instantiate the `Dist` abstract class
        results in a `TypeError`.

        """
        with self.assertRaises(TypeError):
            backend.Dist()

    def test_subclass_without_method_error(self):
        """
        Test case to check that creation of a
        subclass without implementing `processAndMerge`
        method throws a `TypeError`.

        """
        class TestBackend(backend.Dist):
            pass

        with self.assertRaises(TypeError):
            TestBackend()

class DistBuildRangesTest(unittest.TestCase):
    """
    Tests to check the working of the `BuildRanges`
    instance method in `Dist` class.

    """
    class TestBackend(backend.Dist):
        """
        A Dummy backend class to test the
        BuildRanges method in Dist class.

        """
        def ProcessAndMerge(self, mapper, reducer):
            """
            Dummy implementation of ProcessAndMerge.

            """
            pass

    def test_nentries_multipleOf_npartitions(self):
        """
        Test cases that check the working of `BuildRanges`
        method when `nentries` is a multiple of `npartitions`.

        """
        backend = DistBuildRangesTest.TestBackend()
        nentries_small = 10
        npartitions_small = 5
        nentries_large = 100
        npartitions_large = 10

        ranges_small = backend.BuildRanges(nentries_small, npartitions_small)
        ranges_large = backend.BuildRanges(nentries_large, npartitions_large)

        ranges_small_reqd = [(0, 2), (2, 4), (4, 6), (6, 8), (8, 10)]
        ranges_large_reqd = [
        (0, 10),
        (10, 20),
        (20, 30),
        (30, 40),
        (40, 50),
        (50, 60),
        (60, 70),
        (70, 80),
        (80, 90),
        (90, 100)
        ]

        self.assertListEqual(ranges_small, ranges_small_reqd)
        self.assertListEqual(ranges_large, ranges_large_reqd)

    def test_nentries_not_multipleOf_npartitions(self):
        """
        Test cases that check the working of `BuildRanges` method
        when `nentries` is not a multiple of `npartitions`.

        """
        backend = DistBuildRangesTest.TestBackend()
        nentries_1 = 10
        npartitions_1 = 4
        nentries_2 = 9
        npartitions_2 = 4

        # Example in which fractional part of
        # (nentries/npartitions) >= 0.5
        ranges_1 = backend.BuildRanges(nentries_1, npartitions_1)
        # Example in which fractional part of
        # (nentries/npartitions) < 0.5
        ranges_2 = backend.BuildRanges(nentries_2, npartitions_2)

        # Required output pairs
        ranges_1_reqd = [(0, 3), (3, 6), (6, 8), (8, 10)]
        ranges_2_reqd = [(0, 3), (3, 5), (5, 7), (7, 9)]

        self.assertListEqual(ranges_1, ranges_1_reqd)
        self.assertListEqual(ranges_2, ranges_2_reqd)

    def test_nentries_greater_than_npartitions(self):
        """
        Test case that check the working of `BuildRanges` method
        when the value of `nentries` is lesser than `npartitions`.

        """
        backend = DistBuildRangesTest.TestBackend()
        nentries = 5
        npartitions = 7 # > nentries

        ranges = backend.BuildRanges(nentries, npartitions)

        ranges_reqd = [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]

        self.assertListEqual(ranges, ranges_reqd)
