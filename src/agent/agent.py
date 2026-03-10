from abc import ABC, abstractmethod
from pydantic import BaseModel
from pydantic_ai import BinaryContent
from typing import Any, Union, List


# Define an abstract base class for sentiment analysis agents
class IeltsReadingAgent(ABC):
    @abstractmethod
    def analyze(self, content: Union[str, BinaryContent]) -> Any:
        pass

class IeltsSpeakingAgent(ABC):
    @abstractmethod
    def analyze(self, content: Union[str, BinaryContent]) -> Any:
        pass

class IeltsWritingAgent(ABC):
    @abstractmethod
    def analyze(self, content: Union[str, BinaryContent]) -> Any:
        pass

class IeltsListeningAgent(ABC):
    @abstractmethod
    def analyze(self, content: Union[str, BinaryContent]) -> Any:
        pass