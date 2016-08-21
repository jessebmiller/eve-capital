from etl.extractors import (
    CrestOrderSnapshotExtractor,
    CrestMarketHistoryExtractor,
)
import sqlite3
from contextlib import contextmanager
import pandas as pd

@contextmanager
def sde():
    with sqlite3.connect("/eve-sde/dump-2016-07-04_19:24.sqlite") as conn:
        cur = conn.cursor()
        yield cur
        cur.close()

with sde() as cur:
    cur.execute("select regionID from mapRegions where regionName = ?",
                ('The Forge',))
    regions  = [r[0] for r in cur.fetchall()]
    cur.execute("select typeID from invTypes where marketGroupID not null")
    items = [i[0] for i in cur.fetchall()]

histories = CrestMarketHistoryExtractor(regions, items)()

hist = pd.DataFrame(histories.data())

avg_price_variance = hist[['typeID', 'avgPrice']].groupby('typeID').var()
avg_price_variance.rename(columns={'avgPrice': 'avg_price_var'}, inplace=True)
avg_price_mean = hist[['typeID', 'avgPrice']].groupby('typeID').mean()
avg_price_mean.rename(columns={'avgPrice': "avg_price_mean"}, inplace=True)
volume_var = hist[['typeID', 'volume']].groupby('typeID').var()
volume_var.rename(columns={'volume': 'volume_var'}, inplace=True)
volume_mean = hist[['typeID', 'volume']].groupby('typeID').mean()

scores = pd.concat(
    [volume_var, volume_mean, avg_price_variance, avg_price_mean],
    join='inner',
    axis=1,
)

volume_mult = scores['volume'] / (scores['volume_var'] + 1)
scores['score'] = scores['avg_price_var'] * volume_mult

M = 1000000
affordable = scores[scores['avg_price_mean'] < 100 * M]

def lookup_name(typeID):
    with sde() as c:
        c.execute("select typeName from invTypes where typeID=?", (typeID,))
        name = c.fetchone()
    return name

print(sorted(map(lookup_name, affordable.sort('score').index))[-10:])

import pdb; pdb.set_trace()


