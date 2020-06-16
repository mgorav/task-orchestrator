from unittest import TestCase

from moksh_orchestrator.framework.apis.orchestrator import Orchestrator
from moksh_orchestrator.framework.apis.step import Step
from moksh_orchestrator.framework.impl.orchestrator_impl import OrchestratorImpl
from moksh_orchestrator.framework.impl.step_impl import StepImpl
from moksh_orchestrator.framework.moksh import Moksh
from moksh_orchestrator.framework.utils.file_utils import find_file


class MokshTest(TestCase):
    setup = False
    moksh = None
    file = ""

    def setUp(self):

        if not MokshTest.setup:
            MokshTest.file = find_file('task1.yaml')
            MokshTest.moksh = Moksh(self.file)
            MokshTest.setup = True

    def test_lookup_moksh_configuration_yaml(self):
        self.assertIsNotNone(self.file)

    def test_moksh_creation(self):

        self.assertIsNotNone(self.moksh)

    def test_orchestrator(self):

        orchestrator: Orchestrator = self.moksh.get_orchestrator()
        self.moksh.run()

        self.assertTrue(Orchestrator.providedBy(orchestrator))

        self.assertTrue(Orchestrator.implementedBy(OrchestratorImpl))
        self.assertTrue(isinstance(orchestrator, OrchestratorImpl))
        self.assertEqual(len(orchestrator.get_steps()), 3)
        for step in orchestrator.get_steps():
            self.assertTrue(Step.implementedBy(StepImpl))
            self.assertTrue(isinstance(step, StepImpl))

    def test_steps(self):
        moksh = Moksh(self.file)
        orchestrator: Orchestrator = moksh.get_orchestrator()

        self.assertTrue(Orchestrator.implementedBy(OrchestratorImpl))
        self.assertTrue(isinstance(orchestrator, OrchestratorImpl))
        self.assertEqual(len(orchestrator.get_steps()), 3)
        for step in orchestrator.get_steps():
            self.assertTrue(Step.implementedBy(StepImpl))
            self.assertTrue(isinstance(step, StepImpl))
