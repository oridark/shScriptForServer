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


class opsApiClient:
    def __init__(self, apikey):
        logger.info(f"{YELLOW}Logging...")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": 'token ' + apikey
            }
        )
        self.base = "https://orpheus.network/"

    def api_call(self, epoint, **kwargs):
        if epoint == "ajax.php?action=browse":
            epoint = "ajax.php?action=browse&searchstr="+kwargs["searchstr"]+"&filter_cat=1"
            params = {}
        elif epoint == "ajax.php?action=artist":
            epoint = "ajax.php?artistname="+kwargs["artistName"]+"&action=browse"
            params = {}
        else:
            params = kwargs
        try:
            r = self.session.get(self.base + epoint, timeout=10, params=params)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(e)
            return None

    def searchSource(self, searchstr):
        return self.api_call("ajax.php?action=browse", searchstr=searchstr)
    def searchSourceByArtist(self, artistName):
        return self.api_call("ajax.php?action=artist", artistName=artistName)
