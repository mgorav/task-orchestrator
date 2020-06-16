import subprocess
import sys

code = """

from metaflow import FlowSpec, step


class HelloFlow(FlowSpec):
 
    @step
    def start(self):
     
        print("HelloFlow is starting.")
        self.next(self.hello)

    @step
    def hello(self):
   
        print("Metaflow says: Hi!")
        self.next(self.end)

    @step
    def end(self):
  
        print("HelloFlow is all done.")


if __name__ == '__main__':
    HelloFlow()

"""

print(subprocess.call(['python','meta-flow-execute.py','run']))

# print(subprocess.call(['python','-c',code,'run']))
