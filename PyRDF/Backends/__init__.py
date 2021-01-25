import importlib
import pkgutil

for _, module_name, _ in pkgutil.walk_packages(__path__):
    module = importlib.import_module(__name__ + "." + module_name)
    del module_name  # Remove loop variable clattering the namespace
del module  # Remove module variable clattering the namespace
