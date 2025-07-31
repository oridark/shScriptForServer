# Wrapper for Qo-DL Reborn. This is a sligthly modified version
# of qopy, originally written by Sorrow446. All credits to the
# original author.

import hashlib
import logging
import time
import requests
from exceptions import (
    AuthenticationError,
    IneligibleError,
    InvalidAppIdError,
    InvalidAppSecretError,
    InvalidQuality,
)
from color import GREEN, YELLOW

logger = logging.getLogger(__name__)


class ggnApiClient:
    def __init__(self, apikey):
        logger.info(f"{YELLOW}Logging...")
        self.session = requests.Session()
        self.session.headers.update(
            {
                'X-API-Key': apikey,
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Encoding': 'gzip,deflate,sdch',
                'Accept-Language': 'en-US,en;q=0.8',
                'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.47"
            }
        )
        self.base = "https://gazellegames.net/"

    def api_call(self, epoint, **kwargs):
        if epoint == "api.php?request=search":
            epoint = "api.php?request=search&search_type=torrents&groupname="+kwargs["groupname"]
            params = {}
        else:
            params = kwargs
        try:
            r = self.session.get(self.base + epoint, timeout=10, params=params)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            print(e)

    def searchSource(self, groupname):
        return self.api_call("api.php?request=search", groupname=groupname)
