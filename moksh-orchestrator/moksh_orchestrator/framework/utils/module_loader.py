import importlib


class ModuleLoader:

    @staticmethod
    def load_instance(clazz, *args):
        splits = clazz.rsplit('.', 1)
        module = splits[0] if len(splits) > 1 else __name__
        cls = splits[1] if len(splits) > 1 else splits[0]

        ctor = getattr(importlib.import_module(module), cls) if module else getattr(module, cls)

        obj = ctor(*args) if args else ctor()

        return obj

    @staticmethod
    def load_module(clazz):
        splits = clazz.rsplit('.', 1)
        module = splits[0] if len(splits) > 1 else __name__
        cls = splits[1] if len(splits) > 1 else splits[0]
        importlib.import_module(module)
