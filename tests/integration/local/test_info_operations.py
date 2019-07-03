import unittest
import PyRDF
import ROOT


class InfoOperationsLocalTest(unittest.TestCase):
    """
    Check that Info operations return the expected result rather than a proxy.
    """

    def test_GetColumnNames(self):
        """
        GetColumnNames returns ROOT string vector without running the event
        loop.
        """
        rdf = PyRDF.RDataFrame(1)
        d = rdf.Define('a', 'rdfentry_').Define('b', 'a*a')

        column_names = d.GetColumnNames()
        expected_columns = ROOT.std.vector('string')()
        expected_columns.push_back("a")
        expected_columns.push_back("b")

        for column, expected in zip(column_names, expected_columns):
            self.assertEqual(column, expected)

    def test_GetColumnType(self):
        """
        GetColumnType returns the type of a given column as a string.
        """
        rdf = PyRDF.RDataFrame(1)
        d = rdf.Define('a', 'rdfentry_').Define('b', 'a*a')

        a_typename = d.GetColumnType('a')
        b_typename = d.GetColumnType('b')
        expected_type = 'ULong64_t'

        self.assertEqual(a_typename, expected_type)
        self.assertEqual(b_typename, expected_type)

    def test_GetDefinedColumnNames(self):
        """
        GetDefinedColumnNames returns the names of the defined columns.
        """
        rdf = PyRDF.RDataFrame(1)
        d = rdf.Define('a', 'rdfentry_').Define('b', 'a*a')

        column_names = d.GetColumnNames()
        expected_columns = ROOT.std.vector('string')()
        expected_columns.push_back("a")
        expected_columns.push_back("b")

        for column, expected in zip(column_names, expected_columns):
            self.assertEqual(column, expected)

    def test_GetFilterNames(self):
        """
        GetFilterNames returns the names of the filters created.
        """
        rdf = PyRDF.RDataFrame(1)
        filter_name = 'custom_filter'
        d = rdf.Filter('rdfentry_ > 1', filter_name)

        filters = d.GetFilterNames()
        expected_filters = ROOT.std.vector('string')()
        expected_filters.push_back(filter_name)

        for f, expected in zip(filters, expected_filters):
            self.assertEqual(f, expected)
