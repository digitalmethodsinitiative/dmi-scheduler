from logging import getLogger, Handler, Formatter, Filter
import platform
import sys
import json
import requests
logger = getLogger(__name__)


class SlackHandler(Handler):
    """
    SlackHandler instances dispatch logging events to Slack Incoming Webhook.
    :param webhook_url: Slack Incoming Webhook URL.
    """
    def __init__(self, webhook_url):
        super().__init__()

        self.url = webhook_url

        self.setFormatter(SlackFormatter())

    def emit(self, record):
        # Try to locate error
        try:
            frames = []
            frame_index = 2
            while True:
                try:
                    frame = sys._getframe(frame_index)
                    frames.append(frame)
                except ValueError:
                    break
                frame_index += 1

            location = "`%s`" % "` ← `".join(
                        [frame.f_code.co_filename.split("/").pop() + ":" + str(frame.f_lineno) for frame in frames])
        except AttributeError:
            # the _getframe method may not be available
            location = "Unknown"
            frames = []

        # Format record
        if isinstance(self.formatter, SlackFormatter):
            attachment = self.format(record)
        else:
            attachment = {'text': self.format(record)}

        attachment["fields"] = [{
                                "title": "Location",
                                "value": location,
                                "short": False
                                }]

        payload = {
            "text": "4CAT Alert logged on `%s`:" % platform.uname().node,
            'attachments': [
                attachment
            ]
        }

        data = json.dumps(payload).encode('utf-8')
        try:
            e = requests.post(self.url, data)
        except requests.RequestException as e:
            # Log error somewhere
            logger.error('Failed to process task: %s', str(e))

class SlackFormatter(Formatter):
    """
    SlackFormatter instances format log record and return a dictionary that can
    be sent as a Slack message attachment.
    :param attr: custom attachment parameters to record attributes dictionary
    :param lvl_color: custom record levels to colors dictionary
    """
    def __init__(self, attr={}, lvl_color={}):
        super().__init__()

        self.level_to_color = {
            'DEBUG':    '#AC92EC',
            'INFO':     '#3CC619', # green
            'WARNING':  '#DD7711', # orange
            'ERROR':    '#FF0000', # red
            'CRITICAL': '#FF0000'
        }
        self.level_to_color.update(lvl_color)

        self.attachment = {
            'mrkdwn_in': ['text'],
            'text': '%(message)s\n',
        }
        self.attachment.update(attr)

    def format(self, record):
        record.message = super(SlackFormatter, self).format(record)
        json_string = json.dumps(self.attachment) % record.__dict__
        # json_string.encode('unicode_escape')

        attachment = json.loads(json_string)
        attachment.update({'color': self.level_to_color[record.levelname]})

        return attachment


class SlackFilter(Filter):
    """
    SlackFilter instances can be use to determine if the specified record is to
    be sent to Slack Incoming Webhook.
    :param allow: filtering rule for log record.
    """
    def __init__(self, allow=False):
        super().__init__()

        self.allow = allow

    def filter(self, record):
        return getattr(record, 'slack', self.allow)
