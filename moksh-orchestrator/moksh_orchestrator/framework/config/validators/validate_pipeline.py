from yamale import yamale
from yamale.validators import DefaultValidators

from moksh_orchestrator.framework.config.validators.orchestrator_type_validator import OrchestratorTypeValidator
from moksh_orchestrator.framework.config.validators.step_type_validator import StepTypeValidator


class ValidatePipeline:

    @staticmethod
    def validate(task_config_path: str) -> None:
        validators = DefaultValidators.copy()  # This is a dictionary
        validators[OrchestratorTypeValidator.tag] = OrchestratorTypeValidator
        validators[StepTypeValidator.tag] = StepTypeValidator

        schema = yamale.make_schema('./task-schema.yaml', validators=validators)
        pipeline_config = yamale.make_data(task_config_path, parser='ruamel')

        yamale.validate(schema, pipeline_config)
