# Run tests on Node
python -m unittest tests.test_node.OperationReadTest
python -m unittest tests.test_node.NodeReturnTest
python -m unittest tests.test_node.DfsTest

# Run tests on Operation
python -m unittest tests.test_operation.ClassifyTest
python -m unittest tests.test_operation.ArgsTest

# Run tests on Proxy
python -m unittest tests.test_proxy.AttrReadTest