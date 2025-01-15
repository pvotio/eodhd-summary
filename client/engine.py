from datetime import datetime, timedelta

from client.eodhd import EODHD
from config import logger, settings


class Engine:

    TOKEN = settings.TOKEN

    def __init__(self):
        self.data = {}
        self.eodhd = EODHD(self.TOKEN)

    def run(self):
        pass
