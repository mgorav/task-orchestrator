from zope.interface import interface


class ExecutionContext(interface.Interface):
    def addParam(key, value):
        pass

    def getParam(key):
        pass

    def getAllParam():
        pass

    def contains(key):
        pass

    def merge(execution_context):
        pass
