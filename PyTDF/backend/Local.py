import ROOT

def local_executor(generator):
    """
    Execution of the event-loop
    in local environment

    """
    mapper = generator.get_callable()

    TChain = ROOT.TChain(generator.root_node.treename)
    # TODO(shravan97) : Change the ctor to RDataFrame's

    for f in generator.root_node.filelist:
        TChain.Add(f)

    TDF = ROOT.Experimental.TDataFrame(TChain)

    output = mapper(TDF)

    node_map = generator.action_node_map

    list(output.values())[0].GetValue() # Trigger event-loop

    for n in node_map:
        node_map[n].value = output[n]