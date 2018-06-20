from .Operation import Operation

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

    def get_callable(self):
        """
        Converts a given graph into
        a callable and returns the same.

        Returns
        -------
        function
            The callable that traverses through
            the given PyRDF graph and executes
            the operations on all nodes.

        """

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
                this should be fed a value.

            Returns
            -------
                tuple
                    This tuple consists of 2 lists, the first one
                    consisting of output values of action nodes and
                    the second one of action nodes in the same order
                    as that of the values list.

            """
            ## TODO : Somehow remove references to any Node object 
            ## for this to work on Spark

            return_vals = []
            return_nodes = []

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
                    return_nodes.append(node_py)

            for n in node_py.children:
                # Recurse through children and get their output
                prev_vals, prev_nodes = mapper(node_cpp, n)

                # Attach the output of the children node
                return_vals.extend(prev_vals)
                return_nodes.extend(prev_nodes)

            return return_vals, return_nodes

        return mapper

    def get_reducer_callable(self):
        """
        A method that returns a 
        generalized reducer function.

        TODO(shravan97)

        """
        pass