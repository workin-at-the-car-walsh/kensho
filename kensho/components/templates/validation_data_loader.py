from kensho import CommonValidationObjectLoader


class ValidationObjectLoader(CommonValidationObjectLoader):
    
    def __init__(self, loader_map, spark_session):
        CommonValidationObjectLoader.__init__(self, loader_map, spark_session)

    @staticmethod
    def example_filter_latest_order_week(df):
        
        latest_order_week = df["order_week"].max()

        return df[df["order_week"] == latest_order_week]