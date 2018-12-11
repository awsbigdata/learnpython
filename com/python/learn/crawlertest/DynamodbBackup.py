#!/bin/python
from __future__ import print_function # Python 2/3 compatibility
import boto3
import time
from datetime import datetime, timedelta


client = boto3.client('dynamodb',region_name='us-east-1')


def backupCleanup(tablename,time):
    response = client.list_backups(TableName=tablename,TimeRangeUpperBound=time)
    for backup in response["BackupSummaries"] :
        print(backup["BackupArn"])
        response = client.delete_backup(
            BackupArn=backup["BackupArn"]
        )
        print("deletion completed "+str(response))

def createBackup():
    date_7_days_ago = datetime.now() - timedelta(days=7)
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    day=time.strftime('%Y-%m-%d')
    try:
        table = dynamodb.Table('ddbBackupTableNames')
        response = table.scan()
        for item in response['Items']:
            tablename=item["tablename"]
            try:
                response = client.create_backup(
                    TableName=tablename,
                    BackupName=tablename+"_"+day
                )
                backupCleanup(tablename,date_7_days_ago)
                print("Backup has been created " + tablename + "_" + day)
            except Exception ,e:
                print("error occurred while creating back up of ddb table : " +tablename+" :\n"  + str(e))

        while 'LastEvaluatedKey' in response:
            response = table.scan()
            for item in response['Items']:
                tablename = item["tablename"]
                try:
                    response = client.create_backup(
                        TableName=tablename,
                        BackupName=tablename + "_" + day
                    )
                    backupCleanup(tablename, date_7_days_ago)
                    print("Backup has been created " + tablename + "_" + day)
                except Exception, e:
                    print("error occurred while creating back up of ddb table : " + tablename + " :\n" + str(e))

                print("Backup has been created " + tablename + "_" + day)
    except Exception,e:
        print("error occurred in backup job " +str(e))



if __name__ == '__main__':
    print("Backup job started ")
    createBackup()
    print("Backup job completed ")