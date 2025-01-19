import threading

from client.eodhd import EODHD
from config import logger, settings


class Engine:

    TOKEN = settings.TOKEN
    THREAD_COUNT = 10

    def __init__(self, tickers):
        self.on = True
        self.data = {}
        self.eodhd = EODHD(self.TOKEN)
        self._parse_tickers(tickers)

    def run(self):
        self.queue = self.tickers.copy()
        threads = []
        for _ in range(self.THREAD_COUNT):
            t = threading.Thread(target=self._worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        self.on = False
        return self.data

    def _worker(self):
        while self.on and len(self.queue):
            ticker = self.queue.pop(0)
            if ticker in self.data or ticker in ["", None]:
                continue

            self.data[ticker] = None

            try:
                self.data[ticker] = self.eodhd.get_fundamental(ticker)
            except ValueError:
                pass
            except Exception:
                logger.error(f"Error fetching dividends data for {ticker}")

    def _parse_tickers(self, tickers):
        self.tickers = [x[0] for x in tickers]
        self.bbg_tickers_map = {x[0]: x[1] for x in tickers}
