from collections import (
    Counter,
    namedtuple,
)
from etl.extractors import (
    ZKillboardRDPExtractor,
    CrestOrderSnapshotExtractor,
)
import datetime
import sqlite3
from contextlib import contextmanager
import requests
import requests_cache

requests_cache.install_cache("sdcache")

@contextmanager
def sde():
    with sqlite3.connect("/eve-sde/dump-2016-07-04_19:24.sqlite") as conn:
        cur = conn.cursor()
        yield cur
        cur.close()

Sale = namedtuple("Sale", ["system_id", "type_id"])
def demand(kills):
    for kill in kills.data():
        yield Sale(kill['solarSystemID'], kill['victim']['shipTypeID'])
        for i in kill['items']:
            yield Sale(kill['solarSystemID'], i['typeID'])


def supply(orders):
    for order in orders.data():
        yield Sale(
            order['solarSystemID'],
            order['type']['id']
        ), order['volume']

with sde() as cur:
    cur.execute("select regionID from mapRegions")
    all_regions = [r[0] for r in cur.fetchall()]

epoc = datetime.datetime.today() - datetime.timedelta(days=2)
kills = ZKillboardRDPExtractor(all_regions, epoc, 1)()
demand_count = Counter(demand(kills))
item_ids = [d.type_id for d, _ in demand_count.most_common(30)]
orders = CrestOrderSnapshotExtractor([10000065, 10000058], item_ids, ["sell"])()
supply_count = Counter()
for s, volume in supply(orders):
    supply_count[s] += volume

supply_adjusted_demand = demand_count - supply_count

print("Biggest Oportunities:")
with sde() as cur:
    for sale, count in supply_adjusted_demand.most_common(5):
        cur.execute(
            "select typeName from invTypes where typeID=?",
            (sale.type_id,),
        )
        item = cur.fetchone()[0]
        cur.execute(
            "select solarSystemName from mapSolarSystems where solarSystemID=?",
            (sale.system_id,),
        )
        system = cur.fetchone()[0]
        print(system, "needs", item, "({})".format(count))


