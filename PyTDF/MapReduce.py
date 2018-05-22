class MapReduce(object):
    """
    Class for performing 
    MapReduce on datasets

    """
    def __init__(self):
        
        self.actions = 0
        self.tfs = 0
        self.operations = []
        self.action_node_map = {}

    def execute_from_functions(self, mapper, reducer):
        """
        Use DistROOT to execute a distributed job
        with the given mapper and reducer functions

        TODO(shravan97)
        """

        pass

    def _dfs(self, node, prev = 't', ops = []):
        """
        Method that does a depth-first
        traversal and takes note of the 
        necessary operations to carry out

        """

        ops_copy = list(ops)

        if not node.operation: # If root
            for n in node.next_nodes:
                self._dfs(n)
            return

        if node.operation.op_type == "a":
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

    def execute_from_graph(self, root_node):
        """
        Method that calls the _get_mapper and 
        _get_reducer methods to pass the mapper
        and reducer functions to DistROOT

        TODO(shravan97)
        """

        # from DistROOT import DistTree
        # dTree = DistTree(root_node.file_list, root_node.treename, root_node.npartitions)
        # reduced_dict = dTree.ProcessAndMerge(self._get_mapper(root_node), self._get_reducer())
        # for val in reduced_dict:
        #     self.action_node_map[val].value = reduced_dict[val]

        pass

    def _get_mapper(self, root_node):
        """
        Function that converts a given
        graph onto a mapper function and 
        returns the same

        """

        root_node._graph_prune()

        self.actions = 0
        self.tfs = 0
        self.operations = []
        self.action_node_map = {}
        self._dfs(root_node)

        def mapper(t):
            ops = {'t':t}
            ret_actions = {}
            for tp in self.operations:
                obj = None
                for i in range(len(tp[2])):
                    if i==0:
                        obj = getattr(ops[tp[1]], tp[2][i].name)(*tp[2][i].args, **tp[2][i].kwargs)
                    else:
                        obj = getattr(obj, tp[2][i].name)(*tp[2][i].args, **tp[2][i].kwargs)

                ops[tp[0]] = obj

                if tp[0][:2] == "ta":
                    ret_actions[tp[0]] = obj


            return ret_actions

        return mapper

    def _get_reducer(self):
        """
        A method that returns a 
        generalized reducer function 

        TODO(shravan97)

        """
        pass