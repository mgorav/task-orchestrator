import yaml
from zope.interface import implementer

from moksh_orchestrator.framework.apis.execution_context import ExecutionContext
from moksh_orchestrator.framework.apis.routable_step import RoutableStep
from moksh_orchestrator.framework.apis.step import Step
from moksh_orchestrator.framework.apis.dataset import Dataset


@implementer([Step, RoutableStep])
class StepImpl(yaml.YAMLObject):
    # yaml_tag = '!step'

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def execute(self, execution_context: ExecutionContext, unit_of_work: Dataset) -> (ExecutionContext, Dataset):
        print('Executing step...{}'.format(self.name))
        uow = unit_of_work.execute(execution_context)
        return execution_context, uow

    # def __repr__(self):
    #     return '%s(name=%r priority=%r type=%r)' % (self.name, self.priority, self.type)
    #
    # def __str__(self):
    #     return 'name = {} priority = {} type = {}'.format(self.name, self.priority, self.type)

    def is_parallel(self):
        return self.parallel

    def get_threads(self):
        return self.threads

    @property
    def get_name(self):
        return self.name

    @property
    def get_priority(self):
        return self.priority

    @property
    def get_type(self):
        return self.type

    def next_step(self):
        pass
