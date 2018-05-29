class Executor(object):

    def check_env(self):
        """
        Method to check the current 
        execution environment from
        PYTDF_ENV variable
        
        """
        
        if not env or (env == "local"):
            return "local"
        return env

    def execute(self, generator):
        """
        Method to execute the 
        event-loop from the root
        node based on the execution
        environment

        """

        import os
        env = os.environ.get('PYTDF_ENV')

        if not env or (env == "local"):
            from .Local import local_executor
            return local_executor(generator)

        else:
            from .Dist import dist_executor
            return dist_executor(generator)