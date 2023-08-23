from kensho import CommonDataLoader
import pandas

class DataLoader(CommonDataLoader):
    def __init__(self, loader_map, spark_session):
        CommonDataLoader.__init__(self, loader_map, spark_session)

    @staticmethod
    def example_method(fp):
        return pandas.read_csv(fp, header=0)