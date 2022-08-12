from src.patterns.application.notification_log import AbstractNotificationLog, Section, format_section_id
from src.patterns.domain_model.notification import ApplicationRecorder


class LocalNotificationLog(AbstractNotificationLog):

    DEFAULT_SECTION_SIZE = 10

    def __init__(self, recorder: ApplicationRecorder, section_size: int = DEFAULT_SECTION_SIZE):
        self.recorder = recorder
        self.section_size = section_size

    def __getitem__(self, section_id: str) -> Section:
        # Interpret the section ID.
        parts = section_id.split(',')
        part1, part2 = int(parts[0]), int(parts[1])
        start = max(1, part1)
        limit = min(max(0, part2 - start + 1), self.section_size)
        # Select notifications.
        notifications = self.recorder.select_notifications(start, limit)
        next_id = None
        return_id = None
        if len(notifications):
            last_id = notifications[-1].id
            return_id = format_section_id(notifications[0].id, last_id)
            # Get next section ID.
            if len(notifications) == limit:
                next_start = last_id + 1
                next_id = format_section_id(next_start, next_start + limit - 1)
        # Return a section of the notification log.
        return Section(section_id=return_id, items=notifications, next_id=next_id)
