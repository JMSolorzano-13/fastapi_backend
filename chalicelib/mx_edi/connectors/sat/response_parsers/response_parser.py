from abc import ABC, abstractmethod


class ResponseParser(ABC):
    @staticmethod
    @abstractmethod
    def parse(response: str) -> dict[str, str]:
        """Parse an event from a source in XML representation."""
