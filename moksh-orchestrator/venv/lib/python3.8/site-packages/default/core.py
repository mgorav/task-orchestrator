import copy

def default(new, base):
    new = {} if new is None else new
    base = {} if base is None else copy.deepcopy(base)

    base.update(new)

    return base
