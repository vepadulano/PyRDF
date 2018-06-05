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

    TDF = ROOT.ROOT.RDataFrame(generator.root_node.treename, filenames_vec)

    output = mapper(TDF)

    node_map = generator.action_node_map

    list(output.values())[0].GetValue() # Trigger event-loop

    for n in node_map:
        node_map[n].value = output[n]