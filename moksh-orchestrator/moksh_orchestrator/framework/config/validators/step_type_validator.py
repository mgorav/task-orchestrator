from yamale.validators import Validator
from moksh_orchestrator.framework.apis.step import Step
from moksh_orchestrator.framework.utils.module_loader import ModuleLoader


class StepTypeValidator(Validator):
    """ Step type check validator """
    tag = 'step_type'

    def _is_valid(self, value):
        return Step.providedBy(ModuleLoader.load_instance(value))

    def fail(self, value):
        return '\'%s\' is not a %s. Step type should implement interface \'%s.%s\' ' % (
            value, self.get_name(), Step.__module__, Step.__name__)
