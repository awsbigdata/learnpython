import  boto3
client = boto3.client('glue')


databaseName='athenademo'
tableName='acralwlerempty'


def delete_oldversion(databaseName,tableName):
    print("deleting {}.{}".format(databaseName,tableName))
    response = client.get_table_versions(
        DatabaseName=databaseName,
        TableName=tableName
    )
    versionids = []
    for table in response['TableVersions']:
        if (len(versionids) % 60 == 0):
            response = client.batch_delete_table_version(
                DatabaseName=databaseName,
                TableName=tableName,
                VersionIds=versionids
            )
            print(response)
    print("completed {}.{}".format(databaseName,tableName))



for tablename in ['','','']:
    delete_oldversion(databaseName,tableName)

print("completed all jobs")