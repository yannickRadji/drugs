from abc import ABC, abstractmethod


class Execute(ABC):
    @abstractmethod
    def parse_args(self, *args):
        pass

    @abstractmethod
    def execute(self, *args):
        pass
