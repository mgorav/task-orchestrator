from yamale.validators import Validator
from moksh_orchestrator.framework.apis.orchestrator import Orchestrator
from moksh_orchestrator.framework.utils.module_loader import ModuleLoader


class OrchestratorTypeValidator(Validator):
    """ Orchestrator type check validator """
    tag = 'pipeline_type'

    def _is_valid(self, value):
        return Orchestrator.providedBy(ModuleLoader.load_instance(value))

    def fail(self, value):
        return '\'%s\' is not a %s. Pipeline type should implement interface \'%s.%s\' ' % (
            value, self.get_name(), Orchestrator.__module__, Orchestrator.__name__)
