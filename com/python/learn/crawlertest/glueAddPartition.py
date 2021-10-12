import sys
import pandas as pd

class GlueSchemaWriter():

    def add_partitions(self, part_list: list):
        """Add partitions to Data Catalog table

        Args:
            part_list (list): List of partitions to be added
        """
        try:
            print(part_list)
            response=self.glue.batch_create_partition(
                DatabaseName=self.db_name,
                TableName=self.table_name,
                PartitionInputList=part_list,
            )
            print(response)
        except self.glue.exceptions.ValidationException as err:
            self.log.error(
                f"Failed to create partition for db:{self.db_name}, table:{self.table_name}"
            )
            self.log.error(err)
            sys.exit(1)


    def _get_table_info(self):
        """
        get Data Catalog table info
        """
        try:
            response = self.glue.get_table(
                DatabaseName=self.db_name, Name=self.table_name
            )
            table_info_dict = {
                "input_format": response["Table"]["StorageDescriptor"]["InputFormat"],
                "output_format": response["Table"]["StorageDescriptor"]["OutputFormat"],
                "table_location": response["Table"]["StorageDescriptor"]["Location"],
                "serde_info": response["Table"]["StorageDescriptor"]["SerdeInfo"],
                "partition_keys": response["Table"]["PartitionKeys"],
                "columns":response["Table"]["StorageDescriptor"]["Columns"]
            }

            return table_info_dict
        except (
                self.glue.exceptions.EntityNotFoundException,
                self.glue.exceptions.InvalidInputException,
                self.glue.exceptions.InternalServiceException,
                self.glue.exceptions.OperationTimeoutException,
        ) as err:
            raise self.log.error(
                f"Failed to get Data Catalog table info for db: {self.db_name}, table: {self.table_name}"
                + f"Error info: {err}"
            )


    def generate_parition_list(self, df: pd.DataFrame, partition_cols: list):
        """
        Generate the list of partitions need to be added to the data catalog table.

        Args:
            df (pd.DataFrame): Pandas dataframe
            partition_cols (list): Partition columns passed as a list
        """

        # Generate unique partition values
        df_parts = df.drop_duplicates(subset=partition_cols)

        # get table info
        table_info = self._get_table_info()
        print(table_info)
        # Return this list
        partition_list = []

        # iterate through unique partition rows to generate partition values
        for index, row in df_parts.iterrows():
            part_loc = [f"{col}={str(row[col])}" for col in partition_cols]
            part_dict = {
                "Values": [str(row[col]) for col in partition_cols],
                "StorageDescriptor": {
                    "Location": f"{table_info['table_location']}/{'/'.join(part_loc)}/",
                    "InputFormat": table_info['input_format'],
                    "OutputFormat": table_info['output_format'],
                    "SerdeInfo": table_info['serde_info'],
                    "Columns": table_info['columns'],
                },
            }
            partition_list.append(part_dict.copy())
        return partition_list




dbname="hive_glue"
tablename="bm_cities_temp1"
region="us-east-1"
import boto3
client = boto3.client('glue',region_name=region)

gs=GlueSchemaWriter()
gs.glue=client
gs.db_name=dbname
gs.table_name=tablename

import awswrangler as wr

df = wr.athena.read_sql_query("SELECT * FROM bm_cities limit 10", database=dbname,workgroup="version2test")
par_col=['year','month','day']
gs.add_partitions(gs.generate_parition_list(df,par_col))
#print(df)
#gs.generate_parition_list()