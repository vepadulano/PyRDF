import unittest, PyRDF, ROOT

class IncludesLocalTest(unittest.TestCase):
    """
    An integration test to check that the required
    header files are properly included.

    """
    def test_includes_function_with_filter_op(self):
        """
        An integration test to check that the filter
        operation is able to use C++ functions that
        were include using header files.

        """
        PyRDF.include("tests/integration/local/test_headers/header1.hxx")

        rdf = PyRDF.RDataFrame(10)

        # This filters out all numbers less than 5
        rdf_filtered = rdf.Filter("check_number_less_than_5(tdfentry_)")
        count = rdf_filtered.Count()

        # The final answer should be the number of integers
        # less than 5, which is 5.
        self.assertEqual(count.GetValue(), 5)