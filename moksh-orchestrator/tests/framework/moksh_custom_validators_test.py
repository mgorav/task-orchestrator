from unittest import TestCase
from yamale import yamale as pipeline_schema_validation
from yamale.validators import DefaultValidators
from moksh_orchestrator.framework.config.validators.orchestrator_type_validator import OrchestratorTypeValidator
from moksh_orchestrator.framework.config.validators.step_type_validator import StepTypeValidator


class TestPipelineSchemaValidations(TestCase):
    schema_def = './task-schema.yaml'
    sample_pipeline_config = './task1.yaml'

    def test_pipeline_schema_validation(self):
        validators = DefaultValidators.copy()  # This is a dictionary
        validators[OrchestratorTypeValidator.tag] = OrchestratorTypeValidator
        validators[StepTypeValidator.tag] = StepTypeValidator

        schema = pipeline_schema_validation.make_schema(self.schema_def, validators=validators)
        data = pipeline_schema_validation.make_data(self.sample_pipeline_config, parser='ruamel')
        try:
            pipeline_schema_validation.validate(schema, data)
        except Exception as e:

            self.fail(e)
