from datetime import datetime

import pandas as pd

from config import settings


class Agent:
    def __init__(self, data):
        self.data = data
