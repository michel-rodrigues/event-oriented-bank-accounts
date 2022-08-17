import abc
from typing import Sequence

from src.patterns.domain_model_layer.domain_event import ImmutableObject
from src.patterns.domain_model_layer.notification import Notification


def format_section_id(first, limit):
    return f'{first},{limit}'


class Section(ImmutableObject):
    section_id: str
    items: Sequence[Notification]
    next_id: str = None


class AbstractNotificationLog(abc.ABC):
    @abc.abstractmethod
    def __getitem__(self, section_id: str) -> Section:
        """Returns section of notification log."""
