import dpath.util
import yaml

code = """
task:
  name: raw-cleansed
  type: "Serial"
  unitOfWork: SparkDataFrame
  executionContext: DictExecutionContext
  steps:
    - step:
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
    - step:
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
    - step:
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


def parse(elem):
    type = elem[1][0]['type']
    print(type)


steps = dpath.util.get(orchestrator, "task/steps")

for step in steps:
   type_of_step = '!!' + step['type']
   dpath.util.set(step,'type',type_of_step)

   type_of_step_exceptionHandler = dpath.util.get(step['exceptionHandler'],'**/type')

   dpath.util.set(step['exceptionHandler'], '**/type', '!!' + type_of_step_exceptionHandler)


print(yaml.dump(orchestrator))
