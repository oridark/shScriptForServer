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


class ptpApiClient:
    def __init__(self, apikey, apiUser):
        logger.info(f"{YELLOW}Logging...")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "ApiUser": apiUser,
                "ApiKey": apikey,
                'Connection': 'keep-alive',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip,deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.47"
            }
        )
        self.base = "https://passthepopcorn.me/"

    def api_call(self, epoint, **kwargs):
        if epoint == "torrents.php?action=groupPage":
            epoint = "torrents.php?page="+kwargs["page"]
            params = {}
        elif epoint == "torrents.php?action=groupPageWithGoldenPopcorn":
            epoint = "torrents.php?page="+kwargs["page"]+"&action=advanced&scene=2"
            params = {}
        elif epoint == "torrents.php?action=groupInfo":
            epoint = "/torrents.php?id="+kwargs["id"]
            params = {}
        elif epoint == "torrents.php?action=download":
            epoint = "/torrents.php?action=download&id="+kwargs["id"]
            params = {}
            return self.downloadFile(epoint, dest=kwargs["dest"])
        else:
            params = kwargs
        try:
            r = self.session.get(self.base + epoint, timeout=20, params=params)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            print(e)

    def downloadFile(self, epoint, **kwargs):
        r = self.session.get(self.base + epoint, timeout=20, params={})
        dest = kwargs.get('dest')  # 获取目标路径，如果没有指定则使用默认路径
        if not dest:
            dest = Path("D:/shared/temp")
        name = re.search(r'filename="(.*)"', r.headers["Content-Disposition"]).group(
            1
        )
        dest = Path(dest, name)
        with dest.open("wb") as fileh:
            fileh.write(r.content)
        return r.content


    def getPtpGroups(self, page):
        return self.api_call("torrents.php?action=groupPage",page = page)
    def getPtpGroupsWithGoldenPopcorn(self, page):
        return self.api_call("torrents.php?action=groupPageWithGoldenPopcorn",page = page)
    def getPtpGroupInfoById(self, id):
        return self.api_call("torrents.php?action=groupInfo", id=id)
    def getPtpTorrentFileById(self, id, dest):
        return self.api_call("torrents.php?action=download", id=id, dest=dest)
