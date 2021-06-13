from abc import ABC, abstractmethod


class Execute(ABC):
    """
    Blueprint for all Execute class that is basically the heart of each application. The main.py is an empty shell that doesn't need to be updated just an entrypoint to the egg.
    """
    @abstractmethod
    def parse_args(self, *args):
        """
        Should parse arguments typically set by an orchestrator

        """
        pass

    @abstractmethod
    def execute(self, *args):
        """

        Args:
            *args: The arguments parsed by pars_args

        """
        pass
