"""
Demand is represented by the loss of an item in a location

Supply is represented by a sell order in a location

"""


from collections import Counter
import requests
import requests_cache
import sqlite3

requests_cache.install_cache("sdcache")

con = sqlite3.connect("dump-2016-07-04_19:24.sqlite")
cur = con.cursor()

cur.execute("select regionId from mapRegions")
all_regions = [r[0] for r in cur.fetchall()]


def kills(regions=all_regions, page_limit=1):
    for page, region_id in [(p, r) for p in range(page_limit) for r in regions]:
        print("getting page of kills")
        kills = requests.get(
            "https://zkillboard.com/api/kills/regionID/{}/page/{}".format(
                region_id, page + 1
            )
        ).json()
        for kill in kills:
            yield kill


def destroyed_items(regions=all_regions):
    for kill in kills(regions):
        for item in kill.get('items', []):
            yield (item.get('typeID', None), kill.get("solarSystemID", None))


def destroyed(type_id, regions=all_regions):
    return (i for i in destroyed_items(regions) if i[0]==type_id)


def item_demand(item_id, regions=all_regions):
    """ returns a counter of demand count by system for the item """
    demand = Counter()
    for i, system in destroyed(item_id, regions):
        assert i == item_id
        demand.update(system)
    return demand


crest = "https://crest-tq.eveonline.com"
sell_order_path = "/market/{regionId}/orders/sell/?type=https://crest-tq.eveonline.com/inventory/types/{typeId}/"
def units_ordered(item, system):
    cur.execute("select regionID, solarSystemName from mapSolarSystems where solarSystemID=?", (system,))
    region, system_name = cur.fetchone()
    units = 0
    next = crest + sell_order_path.format(typeId=item, regionId=region)
    while next:
        resp = requests.get(next).json()
        print("getting", next)
        for order in resp.get('items', []):
            if order['location']['name'][:len(system_name)] == system_name:
                units += order['volume']
        next = resp.get('next', None)
    return units

demand = {}
supply = {}
items = Counter()
for item, system in destroyed_items():
    items.update([item])
    try:
        demand[item].update([system])
    except KeyError:
        demand[item] = Counter([system])

    item_supply = supply.get(item, {})
    system_supply = item_supply.get(system, 0)
    item_supply[system] = system_supply + units_ordered(item, system)
    supply[item] = item_supply

#TOO MUCH, this needs a data model and etl. Just get me a database to play with.
import pdb; pdb.set_trace()

for item, count in items.most_common(10):
    cur.execute(
        "select typeName from invTypes where typeID=?",
        (item,),
    )
    item_name = cur.fetchone()[0]
    print(item_name, count)
    for systemId, count in demand[item].most_common(5):
        cur.execute(
            "select solarSystemName from mapSolarSystems where solarSystemID=?",
            (systemId,),
        )
        system_name = cur.fetchone()[0]
        print("    ", system_name, count)

