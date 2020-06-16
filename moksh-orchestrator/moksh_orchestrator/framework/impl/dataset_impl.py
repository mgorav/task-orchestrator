from typing import TypeVar

from zope.interface import implementer

from moksh_orchestrator.framework.apis.execution_context import ExecutionContext
from moksh_orchestrator.framework.apis.dataset import Dataset

T = TypeVar('T')


@implementer(Dataset)
class DatasetImpl:

    def __init__(self):
        self.cnt = 0;

    def execute(self, execution_context: ExecutionContext) -> ExecutionContext:
        self.cnt = self.cnt + 1
        print('Called {} with execution_context {}'.format(self.__class__.__name__,execution_context))
        print('UOW called {}'.format(self.cnt))
