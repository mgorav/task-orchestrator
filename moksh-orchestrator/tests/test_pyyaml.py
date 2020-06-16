import yaml
from typing import TypeVar, Generic, Mapping, List
from zope.interface import implementer
from zope.interface import implements

from moksh_orchestrator.framework.apis.execution_context import ExecutionContext
from moksh_orchestrator.framework.apis.orchestrator import Orchestrator
from moksh_orchestrator.framework.apis.step import Step
from moksh_orchestrator.framework.apis.dataset import Dataset
from moksh_orchestrator.framework.utils.file_utils import open_file


@implementer(ExecutionContext)
class ExecutionContextImpl:
    pass


@implementer(Dataset)
class UowImpl:
    def __init__(self):
        super().__init__()


@implementer(Step)
class StepImpl(yaml.YAMLObject):
    yaml_tag = '!step'

    def __init__(self, name: str, priority: int, type_of_step: str, **params):
        self.name = name
        self.priority = priority
        self.type = type_of_step
        self.__dict__.update(params)

    def execute(self, execution_context: ExecutionContext, unit_of_work: Dataset) -> (ExecutionContext, Dataset):
        print('Executing step...')
        return execution_context, unit_of_work

    def __repr__(self):
        return '%s(name=%r priority=%r type=%r)' % (self.name, self.priority, self.type)

    def __str__(self):
        return 'name = {} priority = {} type = {}'.format(self.name, self.priority, self.type)

    @property
    def get_name(self):
        return self.name

    @property
    def get_priority(self):
        return self.priority

    @property
    def get_type(self):
        return self.type


class Steps(yaml.YAMLObject):
    yaml_tag = '!steps'

    def __init__(self, steps: [Step]):
        self.steps = steps

    def __repr__(self):
        return '%s(steps=%r)' % self.steps

    def __str__(self):
        return 'steps = {}'.format(self.steps)

    @property
    def get_steps(self):
        return self.steps


@implementer(Orchestrator)
class OrchestratorImpl(yaml.YAMLObject):
    yaml_tag = '!task'

    def __init__(self, name: str, steps: Steps):
        self.name = name
        self.steps = steps

    def __repr__(self):
        return "%s(name=%r, steps=%r)" % (self.name, self.steps,)

    def __str__(self):
        return 'name: {} steps: {}'.format(self.name, self.steps)

    @property
    def get_name(self):
        return self.name

    @property
    def get_steps(self):
        return self.steps


ec = ExecutionContextImpl
uow = UowImpl()

steps = Steps([StepImpl('read-step', 1, 'FileReader', params={'path': 's3'}),
               StepImpl('processor-step', 1, 'Processor', params={'functions': 'dynamodb'})])

dump = yaml.dump(OrchestratorImpl('Serial', steps))

print(dump)

# orchestrator = yaml.full_load("""
# !Orchestrator
# name: Serial
# steps:
#  - name: read-step
#    priority: 1
#    type: FileReader
#  - name: processor-step
#    priority: 2
#    type: Processor
#
# """)
orchestrator = yaml.full_load(dump)
print(type(OrchestratorImpl))
#
code = """
!!python/object:__main__.OrchestratorImpl
  name: raw-cleansed
  orchestrator: SerialOrchestrator
  unitOfWork: SparkDataFrame.
  executionContext: DictExecutionContext
  steps:
    - !!python/object:__main__.StepImpl
      name: read-step
      priority: 1
      type: FileReader
      parameters:
        - key1: value1
      exceptionHandler:
        - name: read-step-error-handler
          type: ReadStepErrorHandler
          parameters:
            - key1: value1
    - !!python/object:__main__.StepImpl
      name: processor-step
      priority: 2
      type: ProcessorStep
      parameters:
        - key1: value1
      exceptionHandler:
        - name: processor-step-error-handler
          type: ProcessorStepErrorHandler
          parameters:
            - key1: value1
    - !!python/object:__main__.StepImpl
      name: writer-step
      priority: 3
      type: WriterStep
      parameters:
        - key1: value1
      exceptionHandler:
        - name: writer-step-error-handler
          type: WriterStepErrorHandler
          parameters:
            - key1: value1



"""
orchestrator = yaml.full_load(code)

# print(orchestrator)

# print(orchestrator)
# print(orchestrator.name)
step1 = orchestrator.get_steps[1]

print(step1)
output = step1.execute(None, None)
