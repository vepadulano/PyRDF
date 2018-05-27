from .Operation import OpTypes

class CallableGenerator(object):
    """
    Class that generates
    mapper abd reducer functions

    """
    def __init__(self):
        
        self.actions = 0
        self.tfs = 0
        self.operations = []
        self.action_node_map = {}

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

        if node.operation.op_type == OpTypes.ACTION:
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


    def get_mapper_callable(self, root_node):
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

    def get_reducer_callable(self):
        """
        A method that returns a 
        generalized reducer function 

        TODO(shravan97)

        """
        pass