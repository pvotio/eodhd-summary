from urllib.parse import urljoin

import requests

from client.request import init_session
from config import logger, settings


class EODHD:

    BASE = "https://eodhistoricaldata.com/api/"

    def __init__(self, token):
        self.token = token
        self.session = init_session(
            settings.REQUEST_MAX_RETRIES, settings.REQUEST_BACKOFF_FACTOR
        )

    def request(self, method, *args, **kwargs):
        headers = {
            "Accept": "*/*",
            "Content-Type": "application/json",
        }
        kwargs["headers"] = headers
        if "params" not in kwargs:
            kwargs["params"] = {}

        kwargs["params"].update(self.params)
        logger.debug(f"Request headers: {headers}")
        logger.debug(f"Request parameters: {kwargs['params']}")

        try:
            response = self.session.request(method, *args, **kwargs)
            if response.status_code == 404:
                raise ValueError("Symbol not found on EODHD API")

            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {args[0]}: {str(e)}")
            raise

    def get_fundamental(self, ticker):
        url = urljoin(self.BASE, f"fundamentals/{ticker}")
        resp = self.request("get", url)
        return resp.json()

    @property
    def params(self):
        logger.debug("Generating request parameters with API token.")
        return {"api_token": self.token, "fmt": "json"}
