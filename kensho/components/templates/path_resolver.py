from kensho import CommonPathResolver

class PathResolver(CommonPathResolver):
    def __init__(self, resolver_map, spark_session):
        CommonPathResolver.__init__(self, resolver_map, spark_session)

    @staticmethod
    def example_method(dataset_name, run_parameters, app_parameters):
        return '{}/{}_{}.csv'.format(
            dataset_name,
            app_parameters['some_key'],
            run_parameters['example_key']
        )