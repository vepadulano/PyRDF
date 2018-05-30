import ROOT

def local_executor(generator):
    """
    Execution of the event-loop
    in local environment

    """
    mapper = generator.get_mapper_callable()
    TChain = ROOT.TChain(generator.root_node.treename)

    for f in generator.root_node.filelist:
        TChain.Add(f)

    TDF = ROOT.Experimental.TDataFrame(TChain)

    generator.root_node.TChain = TChain
    generator.root_node.TDF = TDF

    output = mapper(generator.root_node.TDF)

    node_map = generator.action_node_map

    for n in node_map:
        node_map[n].value = output[n]