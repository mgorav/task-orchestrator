from zope.interface import interface

from moksh_orchestrator.framework.apis.execution_context import ExecutionContext


class Dataset(interface.Interface):
    def execute(execution_context: ExecutionContext) -> ExecutionContext:
        pass
