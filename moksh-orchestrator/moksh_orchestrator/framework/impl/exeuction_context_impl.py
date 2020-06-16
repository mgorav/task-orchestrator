from zope.interface import implementer

from moksh_orchestrator.framework.apis.execution_context import ExecutionContext


@implementer(ExecutionContext)
class ExecutionContextImpl:

    def __init__(self):
        self.params = {}

    def addParam(self, key, value):
        self.params[key] = value

    def getParam(self, key):
        return None if not key in self.params else self.params[key]

    def getAllParam(self):
        return self.params

    def contains(self, key):
        return True if key in self.params else False

    def merge(self, execution_context):
        for key, value in execution_context.getAllParam().items():
            self.addParam(key, value)
