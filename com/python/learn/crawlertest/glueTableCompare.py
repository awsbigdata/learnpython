import boto3

import sys

client = boto3.client('glue',region_name='us-east-1')


databasename=sys.argv[1]
table1=sys.argv[2]
table2=sys.argv[3]


res=client.get_table( DatabaseName=databasename,Name=table1)
res1=client.get_table( DatabaseName=databasename,Name=table2)


def findNext(value):
    stack=0
    leng=0;
    for i in value:
        if('>'==i and stack!=0):
            stack = stack -1;
        elif('<'==i):
            stack=stack+1;
        elif('>'==i and stack==0):
            return leng
        leng=leng+1
    return leng

def createdict(key,val):
    if (val.startswith('array')):
        str=val[val.index('array<')+6:]
        str = str[:findNext(str)]
        return createdict(key,str)
    elif(val.startswith('struct')):
        #print("val",val)
        str = val[val.index('struct<') + 7:]
        str = str[:findNext(str)]
        dictout = {};
        while(len(str)>0):
            #print(dictout)
            #print(str)
            key=str[:str.index(':')]
            str=str[str.index(':')+1:]
            if(str.startswith('array') or str.startswith('struct')):
                if(str.startswith('array')):
                    temp = str[str.index('array<') + 6:]
                    sval = str[:findNext(temp)+7]
                    str = str[findNext(temp) + 8:]
                    dictout[key] = sval
                else:
                    temp = str[str.index('struct<') + 7:]
                    sval = str[:findNext(temp) + 8]
                    str = str[findNext(temp) + 9:]
                    dictout[key] = sval


            else:
                if(',' in str):
                    sval=str[:str.index(',')]
                    str = str[str.index(',') + 1:]
                    dictout[key]=sval;
                else:
                   ## print(str)
                    str=''



        return dictout
    else:
        return {key:val}



def notcommon(dict1,dict2):
   for key in dict1.keys():
       if(key in dict2):
           if(dict1[key]!=dict2[key]):
                if('array' in dict1[key] or 'struct' in dict1[key]):
                    print("inner complex key",key)
                    sd1=createdict(key,dict1[key])
                    sd2=createdict(key,dict2[key])
                    notcommon(sd1,sd2)
                else:
                    print("dtype mismatch table1:{0},{1},table2:{2},{3}".format(key,dict1[key],key,dict2[key]))
       else:
           print("column mismatch table1:{0},{1}".format(key, dict1[key]))


vals='struct<maxEVCModeKey:string,rebootRequired:boolean,at_type:string,quickStats:struct<distributedCpuFairness:int,availablePMemCapacity:int,at_type:string,overallCpuUsage:int,distributedMemoryFairness:int,overallMemoryUsage:int,uptime:int>,host:struct<at_type:string,at_id:string>,managementServerIp:string,runtime:struct<dasHostState:struct<at_type:string,state:string>,bootTime:bigint,at_type:string,connectionState:string,inMaintenanceMode:boolean,networkRuntimeInfo:struct<at_type:string,netStackInstanceRuntimeInfo:array<struct<currentIpV6Enabled:boolean,maxNumberOfConnections:int,vmknicKeys:array<string>,at_type:string,netStackInstanceKey:string,state:string>>,networkResourceRuntime:struct<at_type:string,pnicResourceInfo:array<struct<pnicDevice:string,at_type:string,availableBandwidthForVMTraffic:int,unusedBandwidthForVMTraffic:int>>>>,powerState:string,cryptoState:string,hostMaxVirtualDiskCapacity:bigint,standbyMode:string,inQuarantineMode:boolean,vsanRuntimeInfo:struct<accessGenNo:int,at_type:string>,healthSystemRuntime:struct<systemHealthInfo:struct<at_type:string,numericSensorInfo:array<struct<timeStamp:string,currentReading:int,healthState:struct<summary:string,at_type:string,label:string,key:string>,baseUnits:string,at_type:string,rateUnits:string,sensorType:string,name:string,unitModifier:int,id:string>>>,at_type:string,hardwareStatusInfo:struct<at_type:string,cpuStatusInfo:array<struct<at_type:string,name:string,status:struct<summary:string,at_type:string,label:string,key:string>>>>>>,config:struct<product:struct<at_type:string,fullName:string,localeVersion:string,version:string,apiVersion:string,build:string,vendor:string,licenseProductName:string,name:string,osType:string,localeBuild:string,licenseProductVersion:string,apiType:string,productLineId:string>,vmotionEnabled:boolean,port:int,sslThumbprint:string,at_type:string,faultToleranceEnabled:boolean,name:string>,overallStatus:string,currentEVCModeKey:string,hardware:struct<numHBAs:int,numCpuPkgs:int,at_type:string,cpuModel:string,cpuMhz:int,otherIdentifyingInfo:array<struct<at_type:string,identifierValue:string,identifierType:struct<summary:string,at_type:string,label:string,key:string>>>,numNics:int,uuid:string,numCpuThreads:int,memorySize:bigint,vendor:string,model:string,numCpuCores:int>>'


#print(sd1)
dict1={};

for col in res['Table']['StorageDescriptor']['Columns']:
    dict1[col['Name'].lower()]=col['Type'].lower()

dict2={};
for col in res1['Table']['StorageDescriptor']['Columns']:
    dict2[col['Name'].lower()]=col['Type'].lower()


notcommon(dict1,dict2)

