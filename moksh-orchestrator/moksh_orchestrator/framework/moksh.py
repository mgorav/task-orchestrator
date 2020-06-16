import sys

import dpath.util
import yaml

from moksh_orchestrator.framework.apis.orchestrator import Orchestrator
from moksh_orchestrator.framework.config.validators.validate_pipeline import ValidatePipeline
from moksh_orchestrator.framework.impl.exeuction_context_impl import ExecutionContextImpl
from moksh_orchestrator.framework.utils.file_utils import open_file
from moksh_orchestrator.framework.utils.interface_utils import verify_object_graph
from moksh_orchestrator.framework.utils.module_loader import ModuleLoader
import importlib


class Moksh:

    def __init__(self, path_to_config: str):

        # validate task config
        ValidatePipeline.validate(path_to_config)

        self.orchestrator = self.do_create_orchestrator_from_config(path_to_config)
        verify_object_graph(Orchestrator, self.orchestrator)

    def run(self) -> None:
        # TODO create an ExecutionContext and Dataset based on configured implementations
        # Load and setup self.execution_context self.unit_of_Work
        try:
            execution_context = ExecutionContextImpl()
            self.orchestrator.execute(execution_context)
        except Exception as e:
            # todo add logging decorator
            print(e)
            raise e

    def get_orchestrator(self):
        return self.orchestrator

    def do_create_orchestrator_from_config(self, path: str) -> Orchestrator:
        with open_file(path, 'r') as file:
            return self.do_create_orchestrator_from_dict(yaml.full_load(file))

    def do_create_orchestrator_from_dict(self, orchestrator: dict) -> Orchestrator:

        print('****************************************Configured Plan****************************************')
        print(yaml.dump(orchestrator))
        print('***********************************************************************************************')

        # while traversing the config graph make sure modules/type are not loaded twice
        loaded_modules = []
        # generated task execution plan complying to pyyaml. This will facilitate un-marshalling from YAML
        # to python object - Orchestrator (configured implementation)
        orchestrator_execution_plan = ""

        # find & load correct implementation of Orchestrator. The implementation of Orchestrator will be automatically
        # translated to PYYAML native format i.e. !!python/object[full qualified impl of Orchestrator]
        orchestrator_execution_plan, steps = self.do_construct_pipeline_execution_plan(loaded_modules, orchestrator,
                                                                                       orchestrator_execution_plan)

        self.unit_of_Work = dpath.util.get(orchestrator, "task/datasetType")
        self.execution_context = dpath.util.get(orchestrator, "task/executionContextType")

        if len(steps) > 0:
            orchestrator_execution_plan += '    ' + 'steps:' + '\n'

        # find & load correct implementation of Step. The implementation of Step will be automatically translated
        # to PYYAML native format i.e. !!python/object[full qualified impl of Step]
        for step in steps:
            orchestrator_execution_plan = self.do_construct_steps_execution_plan(loaded_modules,
                                                                                 orchestrator_execution_plan, step)

        # todo add logger decorator
        print('****************************************Execution Plan****************************************')
        print(orchestrator_execution_plan)
        print('**********************************************************************************************')
        return yaml.full_load(orchestrator_execution_plan)

    def do_construct_steps_execution_plan(self, loaded_modules, orchestrator_execution_plan, step) -> str:
        type_of_step = step['type']
        self.load_if_not_already_loaded(type_of_step, loaded_modules)
        full_qualified_type_of_step = '!!python/object:' + type_of_step
        orchestrator_execution_plan += '       - ' + full_qualified_type_of_step + '\n'
        for key, value in step.items():
            if key != 'step':
                orchestrator_execution_plan += '          ' + key + ': ' + str(value) + '\n'

        if 'parallel' not in step:
            orchestrator_execution_plan += '          ' + 'parallel' + ': ' + str('False') + '\n'

        return orchestrator_execution_plan

    def do_construct_pipeline_execution_plan(self, loaded_modules, orchestrator, orchestrator_execution_plan) -> (
            str, []):
        type_of_orchestrator = dpath.util.get(orchestrator, "task/type")
        self.load_if_not_already_loaded(type_of_orchestrator, loaded_modules)
        full_qualified_type_of_orchestrator = '!!python/object:' + type_of_orchestrator
        steps = dpath.util.get(orchestrator, 'task/steps')
        dpath.util.delete(orchestrator, 'task/steps')
        orchestrator_execution_plan += full_qualified_type_of_orchestrator + '\n'
        for key, value in orchestrator['task'].items():
            orchestrator_execution_plan += '    ' + key + ': ' + value + '    \n'
        return orchestrator_execution_plan, steps

    def load_if_not_already_loaded(self, module: str, loaded_modules: []) -> None:
        if not (module in loaded_modules):
            ModuleLoader.load_module(module)
            loaded_modules.append(module)
