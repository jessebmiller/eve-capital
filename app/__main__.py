from etl.extractors import ZKillboardRDPExtractor
import datetime


epoc = datetime.datetime.today() - datetime.timedelta(days=3)
test = ZKillboardRDPExtractor([10000065, 10000058], epoc, 3)

test()
