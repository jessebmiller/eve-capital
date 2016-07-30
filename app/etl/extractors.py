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


class Extractor:

    def expected(self):
        raise NotImplementedError

    def completed(self):
        raise NotImplementedError

    def extract(self, extract_id):
        raise NotImplementedError

    def __call__(self):
        missing_extract_ids = self.expected() - self.completed()
        for extract_id in missing_extract_ids:
            self.extract(extract_id)


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

class ZKillboardRDPExtractor(Extractor):
    """ ZKillboard api calls covering region, day and page spaces """

    def __init__(regions, epoch_date, pages, end_date=datetime.date.max):
        self.regions = regions
        self.epoch_date = epoch_date
        self.pages = pages
        self.end_date = end_date

    def expected(self):
        end_date = min(
            self.end_date,
            datetime.date.today() - datetime.timedelta(days=1),
        )
        tripples = [
            (r, d, p)
            for r in self.regions
            for d in daterange(self.epoch_date, end_date)


