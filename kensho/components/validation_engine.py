from .common import CommonDataLoader, CommonPathResolver, CommonValidationObjectLoader
from .validator import Validator
import pandas
from datetime import datetime
from pytz import timezone
import boto3
import pytz
import warnings

from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.repository import FileSystemMetricsRepository, ResultKey


class ValidationEngine:

    def __init__(
        self,
        path_resolver_class=None,
        data_loader_class=None,
        validation_object_loader_class=None,
        validator_class=None
    ):

        """
        Loads class instance variables
        """
        
        self.generated_input_filepaths = {}
        self.generated_input_objects = {}
        self.generated_validation_objects = {}
        self.generated_validation_results = {}
        self.processed_results = {}
        self.executions = 0
        self.spark = None

        if data_loader_class is None:
            self.data_loader_class = CommonDataLoader
        else:
            self.data_loader_class = data_loader_class

        if path_resolver_class is None:
            self.path_resolver_class = CommonPathResolver
        else:
            self.path_resolver_class = path_resolver_class

        if validation_object_loader_class is None:
            self.validation_object_loader_class = CommonValidationObjectLoader
        else:
            self.validation_object_loader_class = validation_object_loader_class

        if validator_class is None:
            self.validator_class = Validator
        else:
            self.validator_class = validator_class

        # TODO: initialise a spark context
        # TODO: pass the logger in

    def add_input_object(self, name, data_object):
        self.generated_input_objects[name] = data_object
        
    def set_spark(self, spark):
        self.spark = spark

    def load_from(self, json_config, app_parameters, run_parameters):
        """
        This will take the json config and load it into instance variables
        Will also take app and run parameters and load into instance variables
        TODO - implement dependency checks
        """

        config = json_config

        self.data_loader = self.data_loader_class(config['loader_map'], self.spark)  # TODO: is this what we want?
        self.path_resolver = self.path_resolver_class(config['resolver_map'], self.spark)  # TODO: is this what we want?
        self.validation_loader = self.validation_object_loader_class(config['validation_object_loader_map'], self.spark)
        self.validator = self.validator_class()
        
        self.validations = config['validations']

        self.app_parameters = app_parameters
        self.run_parameters = run_parameters

        # list of named inputs
        self.input_objects_to_generate = config['inputs']

        # dict of named validation datasets with config
        self.validation_objects_to_generate = config['validation_objects']
        
    def run_load_input_objects(self, named_input, input_config):
        print('loading input object: ' + named_input)

        filepath = self.path_resolver.call(
                named_input,
                self.run_parameters,
                self.app_parameters,
                input_config
            )
        print(filepath)
        self.generated_input_filepaths[named_input] = filepath

        named_input_object = self.data_loader.call(
            named_input, filepath
        )

        self.generated_input_objects[named_input] = named_input_object
        
    
    def run_load_validation_objects(self, named_validation_dataset, values):
        print('loading validation object: ' + named_validation_dataset)
        curated_input_dfs = {}
        for named_input in values["inputs"]:
            # add required dataframes to new input dict for validation object
            curated_input_dfs[named_input] = self.generated_input_objects[named_input]


        named_validation_object = self.validation_loader.call(named_validation_dataset, curated_input_dfs)

        self.generated_validation_objects[named_validation_dataset] = named_validation_object
    
    def run_validations(self, validation_dataset_name, validation_data_object, validations):
        """
        validations = value of config['validations']
        """
        
        #initial error level pydeequ check_object
        checkError = Check(self.spark, CheckLevel.Error, "Error Checks")
        checkWarning = Check(self.spark, CheckLevel.Warning, "Warning Checks")
        
        #parse rules in config and construct all checks
        for rule in validations[validation_dataset_name]:
            if rule[-1].lower() == "error":
                checkError = self.validator.parse_rule_from_config_and_chain(rule, checkError, validation_dataset_name)
            elif rule[-1].lower() == "warning":
                checkWarning = self.validator.parse_rule_from_config_and_chain(rule, checkWarning, validation_dataset_name)
            else:
                raise ValueError('that is not a good alert level')
            
        #run the pydeequ checks
        result = (
            VerificationSuite(self.spark)
            .onData(validation_data_object.to_spark())
            .addCheck(checkError)
            .addCheck(checkWarning)
            .run()
        )
                
        result_df = result.checkResultsAsDataFrame(self.spark, result).pandas_api()
        #extract rule_id from constraint message TODO improve
        result_df["rule_id"] = result_df["constraint_message"].apply(
            lambda x: x.split(" ")[-1]
        )
        
        
        self.generated_validation_results[validation_dataset_name] = result_df
        
        return result_df

    def run(self):
        
        self.current_execution_number = str(self.executions + 1)
        self.verbose_results = None #remove to make all verbose results available
            
        # Creating a datetime object so we can get int exec ID.
        tz = timezone('Australia/Sydney')
        a = datetime.now(tz)
        current_dt = a.strftime('%Y%m%d%H%M%S')
        
        print('running entire engine')
        if self.run_parameters['pipeline_execution_id'] is not None:
            #create execution ID with syntax <pipeline_exec_id>-1-<datetime> (or whatever execution number this is)
            self.current_execution_id = 'pipeline-' + self.run_parameters['pipeline_execution_id'] + '-' + self.current_execution_number + '-' + current_dt
        else:
            self.current_execution_id = 'manual-execution-' + self.current_execution_number + '-' + current_dt
            print('current execution ID: ' + str(self.current_execution_id))
            
        for named_input, input_config in self.input_objects_to_generate.items():
            self.run_load_input_objects(named_input, input_config)
        
        print('generated input objects')
        print(self.generated_input_objects.keys())

        for named_validation_dataset, values in self.validation_objects_to_generate.items():
            self.run_load_validation_objects(named_validation_dataset, values)
        
        print('generated validation objects')
        print(self.generated_validation_objects.keys())

        # loop over generated validation objects or named validation datasets?
        for named_validation_object, validation_data_object in self.generated_validation_objects.items():
            
            self.run_validations(named_validation_object, validation_data_object, self.validations)
            
            self.validations_results_df = self.merge_validation_results()
            
            self.verbose_results = self.validations_results_df.merge(self.validator.all_rules_df, on='rule_id',how='left')
            
        verbose_results_columns_ordered = [
            'rule_id',
            'validation_dataset',
            'rule_type',
            'column',
            'data_type_name',
            'condition',
            'values',
            'operator',
            'threshold',
            'validation_metric_value',
            # 'check_level',
            # 'check_status',
            'alert_level',
            'constraint_status'
        ]

        self.verbose_results['constraint_status'] = self.verbose_results['constraint_status'].fillna("Success")

        try:
            self.verbose_results = self.verbose_results[verbose_results_columns_ordered]
        except:
            print("Was unable to order verbose results columns")
        
        print('generated validation results')
        print(self.generated_validation_results.keys())

        self.spark.sparkContext._gateway.shutdown_callback_server()
        
        print('finished')
        self.executions = self.executions + 1

        print("Number of executions is now " + str(self.executions))
            
        
    def merge_validation_results(self):
        validation_results_df = pandas.DataFrame(
            self.validator.intercepted_values.items(),
            columns=["rule_id", "validation_metric_value"],
        ).merge(pandas.concat([result_df.to_pandas() for result_df in self.generated_validation_results.values()]), how="left")

        validation_results_df = validation_results_df[
            [
                "rule_id",
                "check_level",
                "check_status",
                "constraint_status",
                "validation_metric_value",
            ]
        ]
        return validation_results_df.sort_values("rule_id")
    
    def write_verbose_results_s3(self):
        s3_prefix = self.app_parameters['s3_prefix']
        bucket = self.app_parameters['default_bucket']
        project_path = self.app_parameters['project_path']
        dataset_name = self.app_parameters['validation_dataset_name'] 
        current_extract_dt = self.run_parameters['effective_extract_dt']
        
        s3_results_path = f"{s3_prefix}://{bucket}/{project_path}/data/validation/{dataset_name}/extract_dt={current_extract_dt}/verbose_validations_{self.current_execution_id}.csv"

        self.verbose_results.to_csv(s3_results_path)
        
        return s3_results_path


    def get_results(self, result_type='dict'):
        if self.executions == 0:
            return 'no results yet, run() me'
        else:
            df = self.verbose_results
            df['constraint_status'] = df['constraint_status'].fillna('Success')
            df_counts = df.groupby(['alert_level', 'constraint_status']).agg(count_rules=('rule_id', 'count'))
            df_counts.columns = df_counts.columns.to_flat_index()
            df1 = df_counts.reset_index()

            total_checks = int(df1.agg({'count_rules': ['sum']})['count_rules'])
            error_success = int(df1[(df1['alert_level'] == 'error') & (df1['constraint_status'] == 'Success')].agg({'count_rules': ['sum']})['count_rules'])
            error_fail = int(df1[(df1['alert_level'] == 'error') & (df1['constraint_status'] == 'Failure')].agg({'count_rules': ['sum']})['count_rules'])
            warning_success = int(df1[(df1['alert_level'] == 'warning') & (df1['constraint_status'] == 'Success')].agg({'count_rules': ['sum']})['count_rules'])
            warning_fail = int(df1[(df1['alert_level'] == 'warning') & (df1['constraint_status'] == 'Failure')].agg({'count_rules': ['sum']})['count_rules'])

            checks_dict = {
                self.executions : {
                "id" : self.current_execution_id,
                "total": total_checks,
                "error_success": error_success,
                "error_fail": error_fail,
                "warning_success": warning_success,
                "warning_fail": warning_fail
                }
            }
            
            checks_df = pandas.DataFrame([['ErrorSuccess', error_success],
                   ['ErrorFail', error_fail],
                   ['WarningSuccess', warning_success],
                   ['WarningFail', warning_fail],
                   ['TotalChecks', total_checks]],
                  columns=['metric_name', 'metric_value'])
            
            if result_type == 'dict':
                return checks_dict
            if result_type == 'dataframe':
                return checks_df
        
    def send_metrics_to_cloudwatch(self, region_name=None):
        """
        This should be moved into an aws specific class or component
        """

        import pytz
        cloudwatch = boto3.client('cloudwatch', region_name=region_name)
        global se, buffer, p
        namespace = '/aws/sagemaker/ml-metrics'
        buffer = []
        cw_response = None
        
        aggregated_results = self.get_results(result_type='dataframe')

        dict = aggregated_results.to_dict('records')
        un = datetime.now(tz=pytz.utc)

        for row in dict:
            rr = self.send_validation_metric(un, row['metric_name'], row['metric_value'], cloudwatch, namespace)
            if rr == 1:
                print('broken')
                break

        self.send_validation_metric(None, None, None, cloudwatch, namespace)

        return cw_response

    def send_validation_metric(self, ts, metric_name, metric_value, cloudwatch, namespace):
        if ts:
            buffer.append( (ts, metric_name, metric_value) )
            if len(buffer) < 50:
                return
        else:
            if not buffer:
                return

        # utc_now = datetime.now(tz=pytz.utc)

        metrics = []
        for m in buffer:
            ts, metric_name, metric_value = m

            metrics.append({
                    'MetricName': metric_name,
                    'Dimensions': [
                      {'Name': 'project', 'Value': 'dcf'},
                      {'Name': 'validation_dataset', 'Value': 'conformed_orders_received_weekly'},
                      {'Name': 'execution_id', 'Value': self.current_execution_id},
                    ],
                    'Timestamp': ts,
                    'Value': metric_value,
                    'Unit': 'None'
                })

        rsp = cloudwatch.put_metric_data(Namespace=namespace, MetricData=metrics)
        if rsp['ResponseMetadata']['HTTPStatusCode'] != 200:
            print(rsp)
            return 1

        buffer.clear()
        
    def supress_warnings(self):
        self.spark.sparkContext.setLogLevel("ERROR")
        warnings.filterwarnings("ignore")
        return 'warnings will be supressed!'