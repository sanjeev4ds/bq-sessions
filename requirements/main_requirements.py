with open("/Users/sanjeevsaini/PycharmProjects/pythonProject/bq-sessions-usecase-final/Required_pipeline/requirement/change_for_project.txt", "r") as requirements_file:
    file_content = requirements_file.read()
    list_file_content = file_content.split('\n')

def get_project():
    project_name= list_file_content[0].split("=")[1]
    return project_name

print(get_project())

def get_input_bucket():
    input_bucket_name= list_file_content[1].split("=")[1]
    return input_bucket_name
print(get_input_bucket())

def get_input_directory():
    input_dir_name = list_file_content[2].split("=")[1]
    return input_dir_name
print(get_input_directory())

def get_output_landing_directory():
    output_landing_dir_name = list_file_content[3].split("=")[1]
    return output_landing_dir_name

def get_moving_directory():
    output_moving_dir_name = list_file_content[4].split("=")[1]
    return output_moving_dir_name

def get_gcs_uri_landing():
    input_landing_gcs_uri = list_file_content[5].split("=")[1]
    return input_landing_gcs_uri

def get_bigquery_raw_table():
    output_bigquery_raw_table = list_file_content[6].split("=")[1]
    return output_bigquery_raw_table

def get_bigquery_raw_table_temp():
    output_bigquery_raw_table_temp = list_file_content[7].split("=")[1]
    return output_bigquery_raw_table_temp

def get_gcs_temp_landing():
    input_landing_gcs_temp = list_file_content[8].split("=")[1]
    return input_landing_gcs_temp

def get_bigquery_consume_table():
    output_bigquery_consume_table = list_file_content[9].split("=")[1]
    return output_bigquery_consume_table