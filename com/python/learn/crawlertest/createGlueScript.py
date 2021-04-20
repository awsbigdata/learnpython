import boto3

glue = boto3.client('glue',region_name='us-east-1')

source_database="athena_training"
source_table="test"
destination_format="json"
destination_path="s3://athenaiad/test"

response = glue.create_script(
    DagNodes=[
        {
            "Id": "datasource0",
            "NodeType": "DataSource",
            "Args": [
                {
                    "Name": "database",
                    "Value": "\"{}\"".format(source_database),
                    "Param": False
                },
                {
                    "Name": "table_name",
                    "Value": "\"{}\"".format(source_table),
                    "Param": False
                },
                {
                    "Name": "transformation_ctx",
                    "Value": "\"datasource0\"",
                    "Param": False
                }
            ],
            "LineNumber": 16
        },

        {
            "Id": "datasink2",
            "NodeType": "DataSink",
            "Args": [
                {
                    "Name": "connection_type",
                    "Value": "\"s3\"",
                    "Param": False
                },
                {
                    "Name": "format",
                    "Value": "\"json\"".format(destination_format),
                    "Param": False
                },
                {
                    "Name": "connection_options",
                    "Value": "{\"path\":"+"\"{}\"".format(destination_path)+"}",
                    "Param": False
                },
                {
                    "Name": "transformation_ctx",
                    "Value": "\"datasink2\"",
                    "Param": False
                }
            ],
            "LineNumber": 26
        }
    ],
    DagEdges=[
        {
            "Source": "datasource0",
            "Target": "datasink2",
            "TargetParameter": "frame"
        }
    ],
    Language='PYTHON'
)

print(response)
