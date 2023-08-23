import pandas
import json
import boto3
from datetime import datetime, timedelta
from datetime import date
from datetime import timedelta
from calendar import THURSDAY
from datetime import datetime


class CommonPathResolver:
    def __init__(self, resolver_map=None, spark_session=None):
        self.resolver_map = resolver_map
        self.spark = spark_session
        pass

    @staticmethod
    def its_a_csv_right_here(dataset_name, run_parameters, app_parameters, input_config):
        return '{}.csv'.format(dataset_name)
    
    @staticmethod
    def path_in_app_params(dataset_name, run_parameters, app_parameters, input_config):
        # print(app_parameters['input_paths'][dataset_name])
        return app_parameters['input_paths'][dataset_name]
    
    @staticmethod
    def full_path_in_config(dataset_name, run_parameters, app_parameters, input_config):
        # print(app_parameters['input_paths'][dataset_name])
        return input_config['full_path']
    
    @staticmethod
    def path_is_effective_dt(dataset_name, run_parameters, app_parameters, input_config):
        # print(run_parameters['effective_extract_dt'])
        return run_parameters['effective_extract_dt']
    
    @staticmethod
    def base_with_partitions_in_app_params(dataset_name, run_parameters, app_parameters, input_config):
        
        current_extract_dt = run_parameters['effective_extract_dt']
        bucket = app_parameters['validation_dataset_bucket']
        #base_path = app_parameters['inputs'][dataset_name]['base_path']
        base_path = input_config['base_path']
        s3_prefix = app_parameters['s3_prefix']
        extract_dt_partition_name = input_config['extract_dt_partition_name']
        
        fp = f"{s3_prefix}://{bucket}/{base_path}/{extract_dt_partition_name}={current_extract_dt}"

        return fp
    
    def base_with_partitions_in_app_params_previous(self, dataset_name, run_parameters, app_parameters, input_config):
        
        bucket = app_parameters['validation_dataset_bucket']
        #base_path = app_parameters['inputs'][dataset_name]['base_path']
        base_path = input_config['base_path']
        s3_prefix = app_parameters['s3_prefix']
        extract_dt_partition_name = input_config['extract_dt_partition_name']
        
        previous_extract_dt = self.get_previous_extract_dt(bucket, 
                                                           base_path, 
                                                           extract_dt_partition_name,
                                                           run_parameters['effective_extract_dt'], 
                                                           #app_parameters['inputs'][dataset_name]['extract_days_diff_limit']
                                                           input_config['extract_days_diff_limit']
                                                          )
        
        fp = f"{s3_prefix}://{bucket}/{base_path}/{extract_dt_partition_name}={previous_extract_dt}"

        return fp
    
    @staticmethod
    def get_previous_extract_dt(bucket, base_path, extract_dt_partition_name, effective_extract_dt, day_diff_limit = None):
        
        s3 = boto3.client('s3')

        effective_dt = datetime.strptime(effective_extract_dt, "%Y%m%d")
        prev_dt = effective_dt - timedelta(7)

        x=0
        while x == 0:
            previous_extract_dt_str = datetime.strftime(prev_dt, "%Y%m%d")
            prefix = f'{base_path}/{extract_dt_partition_name}={previous_extract_dt_str}'
            
            r = s3.list_objects_v2(Bucket=bucket,
                           MaxKeys=2,
                           Prefix =prefix)
            
            # print(r['Prefix'])
            if "Contents" in r:
                x = 1
            else:
                prev_dt = prev_dt - timedelta(1)

        # print(previous_extract_dt_str)
        
        delta = effective_dt - prev_dt
        if day_diff_limit:
            if delta.days > day_diff_limit:
                raise ValueError("previous extract dt {previous_extract_dt_str} is too old for a valid comparison: Failing pipeline")
        
        return previous_extract_dt_str

    def call(self, dataset_name, run_parameters, app_parameters, input_config):
        method_name = self.resolver_map.get(dataset_name)
        if method_name is not None:
            return getattr(self, method_name)(dataset_name, run_parameters, app_parameters, input_config)
        else:
            raise ValueError('{} is not a good dataset_name'.format(dataset_name))


class CommonDataLoader:
    def __init__(self, loader_map=None, spark_session=None):
        self.loader_map = loader_map
        self.spark = spark_session
        pass

    @staticmethod
    def pandas_read_csv(fp):
        return pandas.read_csv(fp)
    
    @staticmethod
    def pandas_read_parquet(fp):
        return pandas.read_parquet(fp)
    
    def spark_read_csv(self, fp):
        return self.spark.read.csv(fp, header=True).pandas_api()
    
    def spark_read_parquet(self, fp):
        return self.spark.read.parquet(fp).pandas_api(index_col='__index_level_0__')
    
    @staticmethod
    def identity_map(fp):
        return fp
    
    @staticmethod
    def get_thursday_before(fp):

        date = datetime.strptime(fp, '%Y%m%d')
        offset = (date.weekday() - THURSDAY) % 7
        thursday = date - timedelta(days=offset)
        thursday_string = thursday.strftime("%Y%m%d")
        
        return thursday_string
    
    def call(self, dataset_name, fp):
        method_name = self.loader_map.get(dataset_name)
        if method_name is not None:
            return getattr(self, method_name)(fp)
        else:
            raise ValueError('{} is not an existing dataset_name in provided config'.format(dataset_name))


class CommonValidationObjectLoader:
    def __init__(self, loader_map=None, spark_session=None):
        self.loader_map = loader_map
        self.spark = spark_session
        pass

    @staticmethod
    def its_already_the_object(validation_object_name, curated_objects_dict):
        return curated_objects_dict[validation_object_name]

    def call(self, validation_object_name, curated_objects_dict):
        method_name = self.loader_map.get(validation_object_name)
        if method_name is not None:
            return getattr(self, method_name)(validation_object_name, curated_objects_dict)
        else:
            raise ValueError('{} is not a existing validation_object_name in provided config'.format(validation_object_name))