from .Operation import Operation

class CallableGenerator(object):
    """
    Class that generates a callable
    to parse a PyRDF graph.

    Attributes
    ----------
    root_node
        Head node of a PyRDF graph.

    """
    def __init__(self, root_node):
        """
        Creates a new `CallableGenerator`.

        Parameters
        ----------
        root_node
            Head node of a PyRDF graph.

        """
        self.root_node = root_node

    def _dfs(self, node, prev = 't', ops = []):
        """
        Method that does a depth-first
        traversal and takes note of the 
        necessary operations to carry out.

        """

        ops_copy = list(ops)

        if not node.operation: # If root
            for n in node.next_nodes:
                self._dfs(n)
            return

        if node.operation.op_type == Operation.Types.ACTION:
            self.actions+=1
            
            ops_copy.append(node.operation)
            
            new_var_name = "ta"+str(self.actions)
            self.operations.append((new_var_name, prev, ops_copy))
            self.action_node_map[new_var_name] = node

        else:
            if len(node.next_nodes) != 1:
                self.tfs+=1

                ops_copy.append(node.operation)
                
                cur = 'tt'+ str(self.tfs)
                self.operations.append((cur, prev, ops_copy))
                new_tuple = (cur, [])
            
            else:
                ops_copy.append(node.operation)
                new_tuple = (prev, ops_copy)

            for n in node.next_nodes:
                self._dfs(n, *new_tuple)


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

        self.root_node.graph_prune()

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
                node_py = self.root_node
            else:
                # Execute the current operation using the output of the parent node (node_cpp)
                node_cpp = getattr(node_cpp, node_py.operation.name)(*node_py.operation.args, **node_py.operation.kwargs)

                if node_py.operation.op_type==Operation.Types.ACTION:
                    # Collect all action nodes in order to return them
                    return_vals.append(node_cpp)
                    return_nodes.append(node_py)

            for n in node_py.next_nodes:
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