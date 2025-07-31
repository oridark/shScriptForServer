# Wrapper for Qo-DL Reborn. This is a sligthly modified version
# of qopy, originally written by Sorrow446. All credits to the
# original author.

import hashlib
import logging
import time
import requests
import re
from pathlib import Path
from exceptions import (
    AuthenticationError,
    IneligibleError,
    InvalidAppIdError,
    InvalidAppSecretError,
    InvalidQuality,
)
from color import GREEN, YELLOW

logger = logging.getLogger(__name__)


class bluApiClient:
    def __init__(self, apikey):
        logger.info(f"{YELLOW}Logging...")
        self.apikey = apikey
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": 'Bearer ' + apikey,
                # 'Accept': 'application/json, text/plain, */*',
                # 'Content-Type': 'application/json',
                # 'Accept-Encoding': 'gzip,deflate',
                # "User-Agent": "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/528.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/535.36 Edg/119.0.2045.47"
            }
        )
        self.base = "https://blutopia.cc"

    def api_call(self, epoint, **kwargs):
        if epoint == "/api/torrents/filter":
            epoint = "/api/torrents/filter?categories[0]=1&api_token="+self.apikey + '&tmdbId=' +kwargs["tmdbId"]
            params = {}
        elif epoint == "/api/torrents":
            epoint = "/api/torrents?api_token="+self.apikey
            params = {}
        elif epoint == "/api/torrents/upload":
            epoint = "/api/torrents/upload?api_token=cLxWbFxTXIDUkjN13UtjO2FMcvKUn9F2xMFWi0pCscy5n57OY55HFNy1vMllHLYm9lc70qRRyvlHT2m3MPemheliq4q0b7Lv4aTS"
            params = kwargs["params"]
            try:
                headers={
                    'Content-Type': 'application/json',
                }
                r = self.session.post('https://mxrc.tech' + epoint, timeout=30, data=params, headers=headers)
                print(r)
                r.raise_for_status()
                # print(r)
                return r.json()
            except requests.exceptions.RequestException as e:
                print(e, r)
                return 'upload fail'
        elif epoint == "/torrents/download/:id":
            epoint = "/torrents/download/"+kwargs["torrentId"]+"?api_token="+self.apikey
            params = {}
            return self.downloadFile(epoint, dest=kwargs["dest"], torrentId = kwargs["torrentId"])
        else:
            params = kwargs
        try:
            r = self.session.get(self.base + epoint, timeout=30, params=params)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            print(e)

    def downloadFile(self, epoint, **kwargs):
        try:
            r = self.session.get(self.base + epoint, timeout=30, params={}, headers={
                'Content-Type': 'text/plain; charset=x-user-defined',
            })
            print(r)
            dest = kwargs.get('dest')  # 获取目标路径，如果没有指定则使用默认路径
            if not dest:
                dest = Path("D:/shared/torrents/bluTorrents")
            name = kwargs["torrentId"] + '.torrent'
            dest = Path(dest, name)
            with dest.open("wb") as fileh:
                fileh.write(r.content)
            return r.content
        except requests.exceptions.RequestException as e:
            print(e)
            return 'fail'
    
    def getBluTorrents(self):
        return self.api_call("/api/torrents")
    
    def filterBluTorrent(self, pageNumber = 1, pageSize = 100, tmdbId = ''):
        return self.api_call("/api/torrents/filter", pageNumber = pageNumber, pageSize = pageSize, tmdbId = tmdbId)
    
    def getBluTorrentFile(self, torrentId = '', dest = ''):
        return self.api_call("/torrents/download/:id", torrentId = torrentId, dest = dest)
    
    def uploadBluTorrent(self, params):
        return self.api_call("/api/torrents/upload", params = params)
    