class CallableGenerator(object):
    """
    Class that generates a callable
    to parse a PyRDF graph.

    Attributes
    ----------
    head_node
        Head node of a PyRDF graph.

    """
    def __init__(self, head_node):
        """
        Creates a new `CallableGenerator`.

        Parameters
        ----------
        head_node
            Head node of a PyRDF graph.

        """
        self.head_node = head_node

    def get_action_nodes(self, node_py=None):
        """
        Recurses through PyRDF graph and collects
        the PyRDF node objects.

        Parameters
        ----------
        node_py (optional)
            The current state's PyRDF node. If `None`,
            it takes the value of `self.head_node`.

        Returns
        -------
        list
            A list of the action nodes of the graph in DFS order, which
            coincides with the order of execution in the callable function.

        """
        return_nodes = []

        if not node_py:
            # In the first recursive state, just set the
            # current PyRDF node as the head node
            node_py = self.head_node
        else:
            if node_py.operation.is_action():
                # Collect all action nodes in order to return them
                return_nodes.append(node_py)

        for n in node_py.children:
            # Recurse through children and collect them
            prev_nodes = self.get_action_nodes(n)

            # Attach the children nodes
            return_nodes.extend(prev_nodes)

        return return_nodes

    def get_callable(self):
        """
        Converts a given graph into
        a callable and returns the same.

        Returns
        -------
        function
            The callable that takes in a PyROOT
            RDataFrame object and executes all
            operations from the PyRDF graph on it
            recursively.

        """

        # Prune the graph to check user references
        self.head_node.graph_prune()

        def mapper(node_cpp, node_py=None):
            """
            The callable that recurses through
            the PyRDF nodes and executes operations
            from a starting (PyROOT) RDF node.

            Parameters
            ----------
            node_cpp
                The current state's ROOT CPP node. Initially
                this should be given in as a PyROOT RDataFrame object.

            node_py (optional)
                The current state's PyRDF node. If `None`,
                it takes the value of `self.head_node`.

            Returns
            -------
            list
                A list of RResultPtr objects in DFS order
                of their corresponding actions in the graph.

            """

            return_vals = []

            if not node_py:
                # In the first recursive state, just set the
                # current PyRDF node as the head node
                node_py = self.head_node
            else:
                # Execute the current operation using the output of the parent node (node_cpp)
                node_cpp = getattr(node_cpp, node_py.operation.name)(*node_py.operation.args, **node_py.operation.kwargs)

                if node_py.operation.is_action():
                    # Collect all action nodes in order to return them
                    return_vals.append(node_cpp)

            for n in node_py.children:
                # Recurse through children and get their output
                prev_vals = mapper(node_cpp, n)

                # Attach the output of the children node
                return_vals.extend(prev_vals)

            return return_vals

        return mapper