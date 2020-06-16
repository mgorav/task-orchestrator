from zope.interface import interface

from moksh_orchestrator.framework.apis.execution_context import ExecutionContext
from moksh_orchestrator.framework.apis.dataset import Dataset


class Step(interface.Interface):

    def execute(execution_context: ExecutionContext, unit_of_work: Dataset) -> (ExecutionContext, Dataset):
        pass

    def is_parallel():
        pass

    def get_threads():
        pass
