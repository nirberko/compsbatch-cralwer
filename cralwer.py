from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, List

from scrapy.crawler import CrawlerProcess
from scrapy import Spider, Request
from scrapy.http import Response, JsonRequest, TextResponse
from dataclasses_json import dataclass_json, LetterCase, config
import json
reqs = 0


@dataclass_json
@dataclass
class _bbox:
    left: float = field(metadata=config(field_name="westLongitude"))
    bottom: float = field(metadata=config(field_name="southLatitude"))
    right: float = field(metadata=config(field_name="eastLongitude"))
    top: float = field(metadata=config(field_name="northLatitude"))

    def __str__(self):
        return f"[{self.left},{self.bottom},{self.right},{self.top}]"

    def split(self, cell_axis_reduction_factor=2):
        cell_width = (self.right - self.left) / float(cell_axis_reduction_factor)
        cell_height = (self.top - self.bottom) / float(cell_axis_reduction_factor)

        for x_factor in range(cell_axis_reduction_factor):
            lc_x = self.left + cell_width * x_factor
            rc_x = lc_x + cell_width

            for y_factor in range(cell_axis_reduction_factor):
                lc_y = self.bottom + cell_height * y_factor
                rc_y = lc_y + cell_height

                yield _bbox(lc_x, lc_y, rc_x, rc_y)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class RegionParameters(object):
    boundaries: _bbox
    clip_polygon: str = "POLYGON((-180 90,180 90,180 -90,-180 -90,-180 90))"
    region_type: str = "customPolygon"


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class Paging(object):
    DEFAULT_PAGE_SIZE = 1000
    FIRST_PAGE = 1
    page_size: int = DEFAULT_PAGE_SIZE
    page_number: int = FIRST_PAGE


class HomeStatus(Enum):
    FOR_RENT = "forRent"
    NEW_CONSTRUCTION = "newConstruction"
    AUCTION = "auction"
    FORECLOSED = "foreclosed"
    FOR_SALE_BY_AGENT = 'fsba'
    FOR_SALE_BY_OWNER = 'fsbo'
    RECENTLY_SOLD = "recentlySold"
    FORECLOSURE = "foreclosure"
    COMING_SOON = "comingSoon"
    PRE_FORECLOSURE = "preforeclosure"
    FSBA = "fsba"


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class SearchBody(object):
    paging: Paging
    region_parameters: RegionParameters
    home_statuses: List[HomeStatus]
    sort_order: str = "recentlyChanged"
    listing_category_filter: str = "all"

    def split(self) -> List["SearchBody"]:
        return [
            SearchBody(paging=Paging(), home_statuses=self.home_statuses, region_parameters=RegionParameters(boundaries=b))
            for b in self.region_parameters.boundaries.split()]


class ZillowSearchSpider(Spider):
    name = "ZillowSearch"
    custom_settings = {
        "CONCURRENT_REQUESTS": 2
    }
    _SEARCH_URL = "https://zm.zillow.com/api/public/v2/mobile-search/homes/search"
    _BBOX = _bbox(-83.741219, 41.522719, -83.357890, 41.774026)

    _HEADERS = {
        "X-Client": "com.zillow.android.rentals",
        "Host": "zm.zillow.com",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Mobile Safari/537.36",
    }


    def start_requests(self):
        search_body = SearchBody(Paging(), RegionParameters(self._BBOX), home_statuses=[HomeStatus.FSBA])

        yield JsonRequest(
            url=self._SEARCH_URL,
            method="POST",
            cb_kwargs={
                "search_body": search_body,
                "split_iteration": 0
            },
            callback=self.parse_response,
            meta={
                "proxy": self.settings.get("proxy")
            },
            body=search_body.to_json(),
            headers=self._HEADERS
        )

    def parse_response(self, response: TextResponse, search_body: SearchBody, split_iteration: int):
        if split_iteration > 2:
            print("passing after 3 iterations")
            return
        global reqs
        reqs += 1
        print(
            f"{reqs} requests, split: {split_iteration} bbox: {search_body.region_parameters.boundaries}, results: {response.json()['searchResultCounts']['totalMatchingCount']}")

        if response.json()["searchResultCounts"]["totalMatchingCount"] == 0:
            for s in search_body.split():
                yield JsonRequest(
                    url=self._SEARCH_URL,
                    method="POST",
                    cb_kwargs={
                        "search_body": s,
                        "split_iteration": split_iteration + 1
                    },
                    callback=self.parse_response,
                    meta={
                        "proxy": self.settings.get("proxy")
                    },
                    body=search_body.to_json(),
                    headers=self._HEADERS,
                    dont_filter=True
                )
        else:
            if response.json()["searchResultCounts"]["totalMatchingCount"] > 1000:
                for s in search_body.split():
                    yield JsonRequest(
                        url=self._SEARCH_URL,
                        method="POST",
                        cb_kwargs={
                            "search_body": s,
                            "split_iteration": split_iteration + 1
                        },
                        callback=self.parse_response,
                        meta={
                            "proxy": self.settings.get("proxy")
                        },
                        body=search_body.to_json(),
                        headers=self._HEADERS,
                        dont_filter=True
                    )

        f = open("json_data.json", "w")
        f.write(response.text)
        f.close()

if __name__ == "__main__":
    process = CrawlerProcess(
        settings={
            "proxy": "http://brd-customer-hl_1453d722-zone-data_center:3wsp9bqp1cog@zproxy.lum-superproxy.io:22225"
        }
    )
    process.crawl(ZillowSearchSpider)
    process.start()