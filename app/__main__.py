from collections import (
    Counter,
    namedtuple,
)
from etl.extractors import (
    ZKillboardRDPExtractor,
    CrestMarketStatsExtractor,
    CrestOrderSnapshotExtractor,
)
import datetime
import sqlite3
from contextlib import contextmanager
import requests
import requests_cache

#requests_cache.install_cache("sdcache")

@contextmanager
def sde():
    with sqlite3.connect("/eve-sde/dump-2016-07-04_19:24.sqlite") as conn:
        cur = conn.cursor()
        yield cur
        cur.close()

ignore_ship_market_groups = [1382, 1815]
ignore_market_groups = [2, 19, 150, 475, 1396, 1922, 1659]
not_demand_groups = ignore_ship_market_groups + ignore_market_groups

demand_cache = {}
def is_demand(type_id):
    try:
        return demand_cache[type_id]
    except:
        pass
    with sde() as cur:
        cur.execute("select marketGroupID from invTypes where typeID=?", (type_id,))
        market_group_id = cur.fetchone()[0]
        while market_group_id:
            cur.execute("select parentGroupID from invMarketGroups where marketGroupID=?", (market_group_id,))
            market_group_id = cur.fetchone()[0]
            if market_group_id in not_demand_groups:
                demand_cache[type_id] = False
                return False
    demand_cache[type_id] = True
    return True


Sale = namedtuple("Sale", ["system_id", "type_id"])
def demand(kills):
    for kill in kills.data():
        print("demand?", kill['victim']['shipTypeID'])
        if is_demand(kill['victim']['shipTypeID']):
            yield (Sale(kill['solarSystemID'], kill['victim']['shipTypeID']), 1)
        for i in kill['items']:
            if is_demand(i['typeID']):
                yield (Sale(kill['solarSystemID'], i['typeID']), i['qtyDestroyed'])

def supply(orders):
    for order in orders.data():
        yield Sale(
            order['solarSystemID'],
            order['type']['id']
        ), order['volume']

with sde() as cur:
    cur.execute("select regionID from mapRegions")
    all_regions = [r[0] for r in cur.fetchall()]
    cur.execute("select regionID from mapRegions where regionName = ?",
                ('The Citadel',))
    regions  = [r[0] for r in cur.fetchall()]

market_stats = CrestMarketStatsExtractor()()
item_prices = {}
item_volume = {}
for stat in market_stats.data():
    item_prices[stat['type']['id']] = stat['adjustedPrice']

epoc = datetime.datetime.today() - datetime.timedelta(days=4)
kills = ZKillboardRDPExtractor(regions, epoc, 5)()
demand_count = Counter()
for sale, count in demand(kills):
    if item_prices.get(sale.type_id, 100000000000) < 5000000:
        demand_count[sale] += int(count * item_prices.get(sale.type_id, 100))
item_ids = [d.type_id for d, _ in demand_count.most_common(50)]
orders = CrestOrderSnapshotExtractor(regions, item_ids, ["sell"])()
supply_count = Counter()
for sale, volume in supply(orders):
    supply_count[sale] += int(volume * item_prices.get(sale.type_id, 100))

supply_adjusted_demand = demand_count - supply_count

print("Biggest Demand:")
with sde() as cur:
    for sale, count in demand_count.most_common(0):
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


print("Biggest Oportunities:")
with sde() as cur:
    for sale, count in supply_adjusted_demand.most_common(100):
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


import pdb; pdb.set_trace()
