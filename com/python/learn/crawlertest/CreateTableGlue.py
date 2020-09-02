
import boto3

glue = boto3.client('glue')

DatasetSchema = [
    {
      "Name": "ordernumber",
      "Type": "bigint"
    },
    {
      "Name": "lineitem",
      "Type": "bigint"
    },
    {
      "Name": "material",
      "Type": "string"
    },
    {
      "Name": "quantity",
      "Type": "bigint"
    },
    {
      "Name": "lastupdatedate",
      "Type": "bigint"
    }
  ]

glue.create_table(
            DatabaseName='hive_glue',
            TableInput={
                'Name': "marineeconomy",
                'StorageDescriptor': {
                    'Columns': DatasetSchema,
                    'Location': "wasb://isak@ayovkkeokdpiaazfunctions.blob.core.windows.net/marine",
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {
                            'separatorChar': ","

                        }
                    },
                },
                'Parameters': {
                    "skip.header.line.count": "1",
                    'classification': 'csv'

                },

                 'PartitionKeys': []

            }
        )

DatasetUpdateSchema = [
    {
      "Name": "ordernumber",
      "Type": "bigint"
    },
    {
      "Name": "lineitem",
      "Type": "bigint"
    },
    {
      "Name": "material",
      "Type": "varchar(250)"
    },
    {
      "Name": "quantity",
      "Type": "bigint"
    },
    {
      "Name": "lastupdatedate",
      "Type": "bigint"
    }
  ]

