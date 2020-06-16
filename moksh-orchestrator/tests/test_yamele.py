import yamale

schema = yamale.make_schema('/Users/gmalho/Documents/Personal/MyGitHub/moksh-orchestrator/moksh-orchestrator/moksh_orchestrator/framework/config/task-schema.yaml', parser='ruamel')

# Create a Data object
data = yamale.make_data('./sample-task.yaml', parser='ruamel')

# Validate data against the schema same as before.
yamale.validate(schema, data)