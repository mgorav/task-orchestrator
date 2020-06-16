from zope.interface.interfaces import Invalid
from zope.interface.verify import verifyObject


def verify_object_graph(a_interface, a_object, **kwargs):
    try:
        return verifyObject(a_interface, a_object, kwargs)
    except Invalid as e:
        raise e
