from etl.extractors import ZKillboardRDPExtractor
import datetime


epoc = datetime.datetime.today() - datetime.timedelta(days=10)
test = ZKillboardRDPExtractor([10000065, 10000058, 10000060], epoc, 5)

test()
