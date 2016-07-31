"""
Extraction

Download data we don't have yet.
Save it in a format that let's us know what we have and what we don't.
Repeat.

ExtractID:
Anything with equality implemented can be an id.

  __eq__(self, other) Bool

Extractor: A
A way to show what extractions we expect to have done at this point, a way to
know what extractions have been done and a way to perform the extracts we are
missing.

  expected() set(ExtractID)
  completed() set(ExtractID)
  extract(ExtractID) Bool (side effect of completing an extract)

Killmails


MarketOrders

MarketStats

"""


import datetime
import json
import os
import requests


class JsonFileExtractor:
    namespace = "jf"
    separator = '|'

    def expected(self):
        raise NotImplementedError

    def completed(self):
        extract_ids = os.listdir(self.base_path)
        return set(extract_ids)

    def extract(self, extract_id):
        raise NotImplementedError

    def data(self):
        for extract_id in self.completed():
            with open(os.path.join(self.base_path, extract_id), 'r') as f:
                d = f.read()
                for x in json.loads(d):
                    yield x

    def __call__(self):
        missing_extract_ids = self.expected() - self.completed()
        print("missing {} extracts from {}".format(
            len(missing_extract_ids),
            self.base_path,
        ))
        for extract_id in missing_extract_ids:
            self.extract(extract_id)
        return self


class CrestOrderSnapshotExtractor(JsonFileExtractor):
    """
    crest api calls creating a snapshot if we don't have a current one

    Snapshots across region, type dimentions

    """

    year = False
    month = False
    day = False
    hour = True
    minute = True
    second = True
    microsecond = True
    base_path = "/extracts/crest_order_snapshot"
    host = "https://crest-tq.eveonline.com"
    path = "/market/{}/orders/{}/?type=https://crest-tq.eveonline.com/inventory/types/{}/"

    def __init__(self, regions, item_ids, order_types=["buy", "sell"]):
        self.item_ids = item_ids
        self.regions = regions
        self.order_types = order_types

    def expected(self):
        time_replacements = {}
        if self.year:
            time_replacements['year'] = 9999
        if self.month:
            time_replacements['month'] = 1
        if self.day:
            time_replacements['day'] = 1
        if self.hour:
            time_replacements['hour'] = 0
        if self.minute:
            time_replacements['minute'] = 0
        if self.second:
            time_replacements['second'] = 0
        if self.microsecond:
            time_replacements['microsecond'] = 0

        snapshot_time = datetime.datetime.now().replace(**time_replacements)
        extract_space = [
            (self.namespace, snapshot_time.isoformat(), str(r), ot, str(i))
            for r in self.regions
            for i in self.item_ids
            for ot in self.order_types
        ]
        extract_ids = (self.separator.join(x) for x in extract_space)
        return set(extract_ids)

    def extract(self, extract_id):
        ns, dt, region_id, o_type, type_id = extract_id.split(self.separator)
        orders = []
        locations = {}
        next = self.host + self.path.format(region_id, o_type, type_id)
        while next:
            print("requesting:", next)
            resp = requests.get(next)
            if resp.status_code != 200:
                continue
            resp = resp.json()
            print("got {} orders".format(len(resp['items'])))
            for order in resp['items']:
                href = order['location']['href']
                if not locations.get(href, None):
                    print("requesting:", href)
                    solar_system_resp = requests.get(href)
                    if solar_system_resp.status_code == 200:
                        locations[href] = solar_system_resp.json()['solarSystem']['id']
                    else:
                        locations[href] = None
                order['solarSystemID'] = locations[href]
            orders += resp['items']
            next = resp.get('next', None)

        with open(os.path.join(self.base_path, extract_id), 'w') as f:
            f.write(json.dumps(orders))
        return True


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


class ZKillboardRDPExtractor(JsonFileExtractor):
    """ ZKillboard api calls covering region, day and page spaces """
    time_fmt = "%Y%m%d%M%S" # weird zkillboard time format.
    base_path = "/extracts/zkillboard_rdp"

    def __init__(self, regions, epoch_datetime, pages, end_datetime=datetime.datetime.max):
        self.regions = regions
        self.epoch_datetime = epoch_datetime.replace(hour=0, minute=0, second=0)
        self.pages = pages
        self.end_date = end_datetime

    def expected(self):
        end_date = min(
            self.end_date,
            datetime.datetime.today() - datetime.timedelta(days=1),
        )
        extract_space = [
            (self.namespace, str(r), d.strftime(self.time_fmt), str(p))
            for r in self.regions
            for d in daterange(self.epoch_datetime, end_date)
            for p in range(self.pages)
        ]
        extract_ids = (self.separator.join(x) for x in extract_space)
        return set(extract_ids)

    def completed(self):
        extract_ids = os.listdir(self.base_path)
        return set(extract_ids)

    def extract(self, extract_id):
        """ extract ids are ns.region.date.page """
        ns, region, dt, page = extract_id.split(self.separator)
        start_time = datetime.datetime.strptime(dt, self.time_fmt).replace(
            hour=0,
            minute=0,
            second=0,
        )
        end_time = datetime.datetime.strptime(dt, self.time_fmt).replace(
            hour=23,
            minute=59,
            second=59,
        )
        api_url = ("https://zkillboard.com/api/kills/"
                   "regionID/{}/page/{}/startTime/{}/endTime/{}/").format(
                       region,
                       str(int(page) + 1), # zkillboard does not start at 0
                       start_time.strftime(self.time_fmt),
                       end_time.strftime(self.time_fmt),
                   )
        print("requesting:", api_url)
        response = requests.get(api_url)
        if response.status_code != 200:
            return False
        killmails = response.json()
        print("got {} killmails".format(len(killmails)))
        path = "{}/{}".format(self.base_path, extract_id)
        with open(path, "w") as f:
            f.write(json.dumps(killmails))
        return True
