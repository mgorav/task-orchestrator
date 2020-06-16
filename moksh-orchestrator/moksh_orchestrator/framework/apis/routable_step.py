from zope.interface import interface


class RoutableStep(interface.Interface):

    def next_step():
        pass
