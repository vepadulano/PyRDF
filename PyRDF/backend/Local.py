import ROOT

def local_executor(generator):
    """
    Execution of the event-loop
    in local environment

    """
    mapper = generator.get_callable()

    filenames_vec = ROOT.std.vector('string')()
    for f in generator.root_node.filelist:
        filenames_vec.push_back(f)

    rdf = ROOT.ROOT.RDataFrame(generator.root_node.treename, filenames_vec)

    values, nodes = mapper(rdf)

    values[0].GetValue() # Trigger event-loop

    for i in range(len(values)):
        nodes[i].value = values[i]