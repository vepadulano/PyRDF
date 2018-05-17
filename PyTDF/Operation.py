from __future__ import print_function

class Operation(object):
    
    """
    A Generic representation
    of an operation such as 
    transformations and actions

    """

    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.op_type = self._classify_operation(name)

    def _classify_operation(self, name):
        operations_dict = {
        'Define':'t',
        'Filter':'t',
        'Range':'t',
        'Histo1D':'a',
        'Histo2D':'a',
        'Histo3D':'a',
        'Profile1D':'a',
        'Profile2D':'a',
        'Profile3D':'a',
        'Count':'a',
        'Min':'a',
        'Max':'a',
        'Mean':'a',
        'Sum':'a',
        'Fill':'a',
        'Take':'a'
        }
        
        try:
            return operations_dict[name]
        except Exception as e:
            return None
