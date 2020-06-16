from distributed import Client
from moksh_orchestrator.framework.apis.execution_context import ExecutionContext
from moksh_orchestrator.framework.apis.step import Step
from moksh_orchestrator.framework.apis.dataset import Dataset


class ParallelExecutor:

    def __init__(self, step, execution_context, unit_of_work):
        self.step: Step = step
        self.execution_context: ExecutionContext = execution_context
        self.unit_of_work: Dataset = unit_of_work

    def submit(self):

        client = Client(processes=False)
        futures = []
        for i in range(0, self.step.get_threads(), 1):
            print('Launching thread {}'.format(str(i)))
            self.execution_context.addParam('parallel_exec_cnt', str(i))
            futures.append(client.submit(self.do_submit))

        print('Future results size {}'.format(len(futures)))
        tuples = client.gather(futures)
        # merge the contexts
        for _ in tuples:
            print(_[0].getAllParam())
            self.execution_context.merge(_[0])

    def do_submit(self) -> (ExecutionContext, Dataset):
        self.unit_of_work.execute(self.execution_context)
        return self.step.execute(self.execution_context, self.unit_of_work)
