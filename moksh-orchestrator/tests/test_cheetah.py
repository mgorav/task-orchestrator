from Cheetah.Template import Template

metaflow_class_template = """


#for $imp in $service.imports
$imp

#end for
class $service.class_name (FlowSpec):

  #for $function in $service.functions
    $function
  #end for
  
  if __name__ == '__main__':
    $service.class_name_constructor
    
"""

metaflow_function_template = """

   $decorator
   $function_declaration
       $function_body

"""


class MetaFlowClassMetadata:

    def __init__(self, class_name: str):
        self.imports = []
        self.class_name = class_name
        self.class_name_constructor = '{}()'.format(class_name)
        self.functions = []

    @property
    def get_imports(self):
        return self.imports

    def add_import(self, imp: str):
        self.imports.append(imp)

    @property
    def get_functions(self):
        return self.functions

    def add_function(self, function: str):
        self.functions.append(function)

    @property
    def get_class_name(self):
        return self.class_name


metaflow_class_metadata = MetaFlowClassMetadata('HelloMetaFlow')

metaflow_class_metadata.add_import('from metaflow import FlowSpec, step')




# function_body
for cnt in range(3):
    metaflow_function_template_instance = Template(metaflow_function_template)
    metaflow_function_template_instance.decorator = '@step'

    if cnt == 0:
        metaflow_function_template_instance.function_declaration = 'def start(self):'
        code = """
        print("HelloFlow is starting.")
        self.next(self.hello)
        """
        metaflow_function_template_instance.function_body = code
    elif cnt == 1:
        metaflow_function_template_instance.function_declaration = 'def hello(self):'
        code = """
        print("Metaflow says: Hi!")
        self.next(self.end)
         """
        metaflow_function_template_instance.function_body = code
    elif cnt == 2:
        metaflow_function_template_instance.function_declaration = 'def end(self):'
        code = """
         print("HelloFlow is all done.")
          """
        metaflow_function_template_instance.function_body = code

    metaflow_class_metadata.add_function(metaflow_function_template_instance)

# main metaflow class
metaflow_class_template_instance = Template(metaflow_class_template)
metaflow_class_template_instance.service = metaflow_class_metadata

print(metaflow_class_template_instance)
