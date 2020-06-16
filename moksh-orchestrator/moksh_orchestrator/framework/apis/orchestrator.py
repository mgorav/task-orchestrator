from zope.interface import interface
from moksh_orchestrator.framework.apis.execution_context import ExecutionContext
from moksh_orchestrator.framework.impl.steps import Steps


class Orchestrator(interface.Interface):

    def execute(execution_context: ExecutionContext) -> ExecutionContext:
        pass

    def get_steps() -> Steps:
        pass

    # def blah():
    #     pass
