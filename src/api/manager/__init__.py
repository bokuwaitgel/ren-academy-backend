from importlib import import_module
from pkgutil import iter_modules


for module in iter_modules(__path__):
    if module.name.startswith("_"):
        continue
    import_module(f"{__name__}.{module.name}")