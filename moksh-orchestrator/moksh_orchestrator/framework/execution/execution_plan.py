import dpath


class ExecutionPlan:

    @staticmethod
    def to_execution_plan_from_config(clazz, orchestrator: dict) -> str:
        steps = dpath.util.get(orchestrator, "task/steps")

        for step in steps:
            type_of_step = '!!' + step['type']
            dpath.util.set(step, 'type', type_of_step)

            type_of_step_exception_handler = dpath.util.get(step['exceptionHandler'], '**/type')

            dpath.util.set(step['exceptionHandler'], '**/type', '!!' + type_of_step_exception_handler)
