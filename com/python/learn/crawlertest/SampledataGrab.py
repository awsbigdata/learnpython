
import random
import string
import os
import datetime
import json
import subprocess
import time

def create_file(partitions=10000):
    indate = 1
    date= datetime.datetime(1980, 11, 8, 10, 11, 12)
    for i in range(0,partitions) :
        path_dt=date+datetime.timedelta(hours=indate)
        indate+=1;
        print(path_dt)
        output_path=createOutputDir(path_dt)
        subprocess.call(["cp", "/home/shiva/Downloads/grab-incentives-part-00000-8f5ea214-4f49-452a-b124-f9025692ea00-c000.snappy.parquet",output_path])
        #time.sleep(1)


def createOutputDir(date):
    output_path = "/tmp/test/output/year={0}/month={1}/day={2}/hour={3}/".format(date.year, date.month, date.day,date.hour)
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    return output_path

import sys
print(sys.version_info)
#print(round(random.uniform(100000, 1000000),3))
create_file(partitions=10000);
print("completed")