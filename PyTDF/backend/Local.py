import ROOT

def local_executor(generator):
    """
    Execution of the event-loop
    in local environment

    """
    mapper = generator.get_mapper_callable()
    generator.root_node.create_tdf()

    output = mapper(generator.root_node._tdf)

    node_map = generator.action_node_map

    for n in node_map:
        node_map[n].value = output[n]