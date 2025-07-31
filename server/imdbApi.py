# Wrapper for Qo-DL Reborn. This is a sligthly modified version
# of qopy, originally written by Sorrow446. All credits to the
# original author.

import hashlib
import logging
import json
import time
import requests
from requests_html import HTMLSession
from exceptions import (
    AuthenticationError,
    IneligibleError,
    InvalidAppIdError,
    InvalidAppSecretError,
    InvalidQuality,
)
from PyMovieDb import ImdbParser
from color import GREEN, YELLOW

logger = logging.getLogger(__name__)


class imdbApiClient:
    def __init__(self):
        logger.info(f"{YELLOW}Logging...")
        self.session = HTMLSession()
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
                "Referer": "https://www.imdb.com/"
            }
        )
        self.baseURL = "https://www.imdb.com"
        self.search_results = {'result_count': 0, 'results': []}
        self.NA = json.dumps({"status": 404, "message": "No Result Found!", 'result_count': 0, 'results': []})

    def api_call(self, epoint, **kwargs):
        if epoint == "ajax.php?action=browse":
            epoint = "ajax.php?action=browse&groupname="+kwargs["searchstr"]+"&filter_cat=1"
            params = {}
        elif epoint == "ajax.php?action=artist":
            epoint = "ajax.php?artistname="+kwargs["artistName"]+"&action=artist"
            params = {}
        else:
            params = kwargs
        try:
            r = self.session.get(self.base + epoint, timeout=10, params=params)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            print(e)

    def getImdbById(self, imdbId):
        url = f"{self.baseURL}/title/{imdbId}"
        """
         @description:- helps to get a file's complete info (used by get_by_name() & get_by_id() )
         @parameter:- <str:url>, url of the file/movie/tv-series.
         @returns:- File/movie/TV info as JSON string.
        """
        try:
            response = self.session.get(url)
            # result = response.html.xpath("//script[@type='application/ld+json']")[0].text
            result = response.html.xpath("//script[@type='application/json']")[0].text
            result = ''.join(result.splitlines())  # removing newlines
            result = f"""{result}"""
        except IndexError:
            print('IndexError')
            return self.NA
        try:
            # converting json string into dict
            result = json.loads(result)
        except json.decoder.JSONDecodeError as e:
            print('json.decoder.JSONDecodeError')
            # sometimes json is invalid as 'description' contains inverted commas or other html escape chars
            try:
                to_parse = ImdbParser(result)
                # removing trailer & description schema from json string
                parsed = to_parse.remove_trailer
                parsed = to_parse.remove_description
                # print(parsed)
                result = json.loads(parsed)
            except json.decoder.JSONDecodeError as e:
                print('json.decoder.JSONDecodeError 2')
                try:
                    # removing reviewBody from json string
                    parsed = to_parse.remove_review_body
                    result = json.loads(parsed)
                except json.decoder.JSONDecodeError as e:
                    print('json.decoder.JSONDecodeError 2')
                    return self.NA

        mainColumnData = result["props"]["pageProps"]["mainColumnData"]
        aboveTheFoldData = result["props"]["pageProps"]["aboveTheFoldData"]
        countries = [d["text"] for d in mainColumnData["countriesOfOrigin"]["countries"] if "text" in d]
        countries = ", ".join(countries)
        output = {
            "type": mainColumnData["titleType"]["id"],
            "name": mainColumnData["titleText"]["text"],
            "country": countries,
            "rating": {
                "ratingCount": aboveTheFoldData["ratingsSummary"]["voteCount"],
                "ratingValue": aboveTheFoldData["ratingsSummary"]["aggregateRating"],
            },
        }
        return output
