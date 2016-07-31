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


class Extractor:

    def expected(self):
        raise NotImplementedError

    def completed(self):
        raise NotImplementedError

    def extract(self, extract_id):
        raise NotImplementedError

    def __call__(self):
        missing_extract_ids = self.expected() - self.completed()
        print("missing {} extracts".format(len(missing_extract_ids)))
        for extract_id in missing_extract_ids:
            self.extract(extract_id)


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


class ZKillboardRDPExtractor(Extractor):
    """ ZKillboard api calls covering region, day and page spaces """
    time_fmt = "%Y%m%d%M%S" # weird zkillboard time format.
    separator = "-"
    namespace = "ns"
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
                       page,
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

