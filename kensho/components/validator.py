import pydeequ
from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.repository import FileSystemMetricsRepository, ResultKey
import operator
import pandas


class Validator:
    def __init__(self):

        self.intercepted_values = dict()
        self.all_rules_df = pandas.DataFrame(columns=['rule_id','rule_type'])

    def squawker(self, rule_name, f):
        def g(x):
            self.intercepted_values[rule_name] = x
            return f(x)

        return g

    def bad_value_anywhere_in_multiple_columns(self, check_object, rule):

        (rule_id, rule_type, bad_value, columns, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','bad_value','columns','threshold', 'alert_level'])

        column_condition = " OR ".join(
            "{} = '{}'".format(c, bad_value) for c in columns
        )

        check_object = check_object.satisfies(
            column_condition,
            rule_id,
            self.squawker(rule_id, lambda x: x <= threshold),
            rule_id,
        )

        return check_object, rule_df

    def column_has_sum_less_than(self, check_object, rule):

        (rule_id, rule_type, column, threshold, alert_level) = rule
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','column','threshold', 'alert_level'])

        check_object = check_object.hasSum(
            column, self.squawker(rule_id, lambda x: x < threshold), rule_id
        )

        return check_object, rule_df

    def column_has_max_less_than(self, check_object, rule):

        (rule_id, rule_type, column, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','column','threshold', 'alert_level'])

        check_object = check_object.hasMax(
            column, self.squawker(rule_id, lambda x: x < threshold), rule_id
        )

        return check_object, rule_df
    
    def has_row_count(self, check_object, rule):

        (rule_id, rule_type, comparison, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','operator','threshold', 'alert_level'])
        
        op = getattr(operator, comparison) # "gt"

        check_object = check_object.hasSize(
            self.squawker(rule_id, lambda x: op(x, threshold)), rule_id
        )

        return check_object, rule_df

    def has_row_count_greater_than(self, check_object, rule):

        (rule_id, rule_type, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','threshold', 'alert_level'])
        
        check_object = check_object.hasSize(
            #self.squawker(rule_id, lambda x: x > threshold), rule_id
        )

        return check_object, rule_df

    def has_row_count_less_than_or_equal_to(self, check_object, rule):

        (rule_id, rule_type, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','threshold', 'alert_level'])

        check_object = check_object.hasSize(
            self.squawker(rule_id, lambda x: x <= threshold), rule_id
        )

        return check_object, rule_df

    def column_has_datatype(self, check_object, rule):

        (rule_id, rule_type, column, data_type_name, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','column', 'data_type_name','threshold', 'alert_level'])

        data_type = getattr(ConstrainableDataTypes, data_type_name, None)

        if data_type is None:
            raise ValueError(
                "There is no {} data type in pydeequ's ConstrainableDataTypes".format(
                    data_type_name
                )
            )

        check_object = check_object.hasDataType(
            column,
            data_type,
            self.squawker(rule_id, lambda x: x >= threshold),
            hint=rule_id,
        )

        return check_object, rule_df

    def column_is_complete(self, check_object, rule):

        (rule_id, rule_type, column, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','column', 'alert_level'])

        check_object = check_object.hasCompleteness(
            column, self.squawker(rule_id, lambda x: x == 1), hint=rule_id
        )

        return check_object, rule_df

    def column_is_nonnegative(self, check_object, rule):

        (rule_id, rule_type, column, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','column','threshold', 'alert_level'])

        check_object = check_object.isNonNegative(
            column, self.squawker(rule_id, lambda x: x >= threshold), hint=rule_id
        )

        return check_object, rule_df

    def column_is_positive(self, check_object, rule):

        (rule_id, rule_type, column, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','column','threshold', 'alert_level'])

        check_object = check_object.isPositive(
            column, self.squawker(rule_id, lambda x: x >= threshold), hint=rule_id
        )

        return check_object, rule_df

    def condition_satisfied_greater_than_or_equal_to_threshold(
        self, check_object, rule
    ):

        (rule_id, rule_type, condition, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','condition','threshold', 'alert_level'])

        check_object = check_object.satisfies(
            condition,
            rule_id,
            self.squawker(rule_id, lambda x: x >= threshold),
            hint=rule_id,
        )

        return check_object, rule_df

    def condition_satisfied_less_than_or_equal_to_threshold(self, check_object, rule):

        (rule_id, rule_type, condition, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','condition','threshold', 'alert_level'])

        check_object = check_object.satisfies(
            condition,
            rule_id,
            self.squawker(rule_id, lambda x: x <= threshold),
            hint=rule_id,
        )

        return check_object, rule_df

    def column_values_are_contained_in(self, check_object, rule):

        (rule_id, rule_type, column, values, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','column','values','threshold', 'alert_level'])

        # TODO: switch back to isContainedIn when pydeequ fix the open issue

        if len(values) > 1:
            column_condition = "{} IN ({})".format(
                column, ", ".join("'{}'".format(value) for value in values)
            )
        else:
            column_condition = "{} = '{}'".format(column, values[0])

        check_object = check_object.satisfies(
            column_condition,
            rule_id,
            self.squawker(rule_id, lambda x: x >= threshold),
            hint=rule_id,
        )

        return check_object, rule_df

    def column_has_number_of_distinct_values_equal_to(self, check_object, rule):

        (rule_id, rule_type, column, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','column','threshold', 'alert_level'])

        check_object = check_object.hasNumberOfDistinctValues(
            column,
            self.squawker(rule_id, lambda x: x == threshold),
            binningUdf=None,
            maxBins=None,
            hint=rule_id,
        )

        return check_object, rule_df

    def column_has_number_of_distinct_values_less_than_or_equal_to(
        self, check_object, rule
    ):

        (rule_id, rule_type, column, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','column','threshold', 'alert_level'])

        check_object = check_object.hasNumberOfDistinctValues(
            column,
            self.squawker(rule_id, lambda x: x <= threshold),
            binningUdf=None,
            maxBins=None,
            hint=rule_id,
        )

        return check_object, rule_df

    def between_two_column_values(self, check_object, rule):

        (rule_id, rule_type, column, column_min, column_max, threshold, alert_level) = rule
        
        rule_df = pandas.DataFrame([rule], columns = ['rule_id', 'rule_type','column','column_min','column_max','threshold', 'alert_level'])

        # TODO: switch back to isContainedIn when pydeequ fix the open issue

        column_condition = "{} is NULL OR ({} <= {} AND {} <= {})".format(
            column, column_min, column, column, column_max
        )

        check_object = check_object.satisfies(
            column_condition,
            rule_id,
            self.squawker(rule_id, lambda x: x >= threshold),
            hint=rule_id,
        )

        return check_object, rule_df

    def parse_rule_from_config_and_chain(self, rule, check_object, validation_dataset_name):
        
        rule_id = rule[0]
        rule_type = rule[1]

        if rule_type in (
            "bad_value_anywhere_in_multiple_columns",
            "column_has_sum_less_than",
            "column_has_max_less_than",
            "has_row_count",
            "has_row_count_greater_than",
            "has_row_count_less_than_or_equal_to",
            "column_has_datatype",
            "column_is_complete",
            "column_is_nonnegative",
            "column_is_positive",
            "condition_satisfied_greater_than_or_equal_to_threshold",
            "condition_satisfied_less_than_or_equal_to_threshold",
            "column_values_are_contained_in",
            "column_has_number_of_distinct_values_equal_to",
            "column_has_number_of_distinct_values_less_than_or_equal_to",
            "column_has_number_of_distinct_values_less_than_or_equal_to",
            "between_two_column_values",
        ):
            check_object, rule_df = getattr(self, rule_type)(check_object, rule)
            rule_df['validation_dataset'] = validation_dataset_name
            
            self.all_rules_df = pandas.concat([self.all_rules_df,rule_df], axis=0, ignore_index=True)
        else:
            raise ValueError(
                rule_type + " not found"
            )  # TODO: just for dev, should be not implemented error

        return check_object