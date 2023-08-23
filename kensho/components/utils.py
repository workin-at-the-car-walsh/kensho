import os
import traceback
import sys

def generate_config_dict():

    config_dictionary = {
            "inputs": {
                "input_data_object_one": {
                    "full_path": "example.csv"
                }
            },
            "resolver_map": {
                "input_data_object_one": "full_path_in_config"
            },
            "loader_map": {
                "input_data_object_one": "spark_read_csv"
            },
            "validation_object_loader_map": {
                "validation_data_object_one": "its_already_the_object"
            },
            "validation_objects":{
                "validation_data_object_one": {
                    "inputs":["input_data_object_one"]
                }
            },
            "validations": {"rules dict goes here"}
        }

    return config_dictionary

def generate_rules_dict():

    #fill out with example of each rule
    rules_dictionary = {
            "current": [
                ["rule-eg-0001", "has_row_count", "ge", 10, "error"],
                ["rule-eg-0002", "column_has_datatype", "names", "String", 1, "error"]
            ]
        }

    return rules_dictionary

def validate_config(config_dictionary):

    result = 'pending'

    #check all keys are there

    #check all validation->inputs are in input key

    #check all rules map to a validator function
    #check all rules have correct schema

    #check all mapped functions exist, validation engine is needed as context

    #check no additional keys are there

    #check all keys in rules dict are in validation objects key

    #check context for inputs matches resolver/loader function expected context VE needed as context

    return result


def generate_template_files(output_dir=''):

    files_list = [
        '__init__.py'
        ,'data_loader.py'
        ,'path_resolver.py'
        ,'validation_data_loader.py'
    ]

    generated_files = []

    #print(os.getcwd()) # C:\Users\darcywalsh\repos\cdo-ds-validation-engine
    #print(os.path.dirname(__file__)) # C:\Users\darcywalsh\repos\cdo-ds-validation-engine\components
    #print(abspath(getsourcefile(lambda:0))) # C:\Users\darcywalsh\repos\cdo-ds-validation-engine\kensho\components\utils.py
    
    cwd = os.getcwd()

    if output_dir != '':
        try:
            output_location = output_dir + '/'
            os.mkdir(cwd + '/'+ output_dir)
        except Exception:
            print(traceback.format_exc())
            # or
            print(sys.exc_info()[2])
            print('couldnt make output directory')
    else:
        output_location = '/'
    
    package_dir = os.path.dirname(__file__).replace('\\','/')

    for file in files_list:
        filepath = f'{cwd}/{output_location}{file}'

        if os.path.exists(filepath):
            print(f'{filepath} already exists, this function will not overwrite')

            return generated_files

        with open(f'{package_dir}/templates/{file}', 'r') as f:
            data = f.read()
            f.close()
        
        filepath = f'{cwd}/{output_location}{file}'
        with open(filepath, 'w') as f:
            f.write(data)
            f.close()
        
        generated_files.append(filepath)
    
    return generated_files