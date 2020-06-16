import yaml

from moksh_orchestrator.framework.apis.step import Step


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
