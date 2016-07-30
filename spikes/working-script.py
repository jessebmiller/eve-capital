from collections import Counter
import requests
import requests_cache
import sqlite3


requests_cache.install_cache('kill-cache')


ship_count = Counter([k['victim']['shipTypeID'] for k in kills])
item_count = Counter()
for kill in kills:
    item_count.update([item['typeID'] for item in kill['items']])

for ship, count in sorted(ship_count.most_common(30), key=lambda x: x[1]):
    cur.execute("select typeName from invTypes where typeID = ?", (ship,))
    name = cur.fetchone()[0]
    print(name, count)

print()
print()

for item, count in sorted(item_count.most_common(30), key=lambda x: x[1]):
    cur.execute("select typeName from invTypes where typeID = ?", (item,))
    name = cur.fetchone()[0]
    print(name, count)


cur.close()
con.close()


# market shit

endpoints = requests.get("https://crest-tq.eveonline.com").json()
crest = "https://crest-tq.eveonline.com"
sell_order_path_template = "/market/{regionId}/orders/sell/?type=https://crest-tq.eveonline.com/inventory/types/{typeId}/"

sell_orders = []
for region_id, type_id in [(r[0], t[0])
                           for r in regions
                           for t in item_count.most_common(1)]:
    next = crest + sell_order_path_template.format(
        regionId=region_id,
        typeId=type_id,
    )
    while next:
        response = requests.get(next).json()
        sell_orders += response.get("items", [])
        next = response.get("next", None)

import pdb; pdb.set_trace()
