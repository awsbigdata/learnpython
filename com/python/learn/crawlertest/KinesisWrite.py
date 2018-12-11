import boto3
import json

print(boto3.__version__)

stream = boto3.client('kinesis')


stream_name = "swerfew"
source_id = 'sweeps-snapesp-prod-valid-1-2018-01-21-00-00-31-05626d76-942c-45af-8565-7c2b6a85b9cd.gz'
partition_key = source_id[:256] # 256 char limit 3:35:37 PM
records = []
one_record = {"Data": "test no.1", "PartitionKey": unicode(partition_key, "utf-8")}
records.append(one_record)
data_2 = '{"provider_id": "136fed7e595025f942a535ddbbca88c7", "last_name": "testlast ", "mvpd": "Comcast_Xfinity", "newsletters": "diy_news,diy_remade,hgtv_outdoors,hpro_consumer,hgtv_marketplace", "campaign_id": 109603, "birth_day": "06", "city": "Feeding hills", "first_name": "testfirst", "zip": "01030", "user_agent_string": "Mozilla/5.0 (iPad; CPU OS 11_2_1 like Mac OS X) AppleWebKit/604.4.7 (KHTML, like Gecko) Version/11.0 Mobile/15C153 Safari/604.1", "custom": {"SP_QUICKEN_LOANS_LEADTYPECODE": "HGTVTEST", "cbh": " http://display.engagesciences.com" , "referrerCode": "direct"}, "state": "MA", "del_required_fields": "", "start_date": "2017-12-27T09:00:00Z", "birth_month": "08", "entry_timestamp": "2018-01-01T00:01:03Z", "brand": "home", "birth_year": "1954", "privacy_policy": "Y", "sweepstakes_name": "HGTV Dream Home 2018", "end_date": "2018-02-16T17:00:00Z", "partners": "", "address1": "123 road", "address2": "", "phone1": "1234567891", "sponsors": "SP_HONDA,SP_LUMBER_LIQUIDATORS,SP_QUICKEN_LOANS,SP_WAYFAIR,SP_BELGARD,SP_CABINETS_TO_GO,SP_DELTA_FAUCET,SP_CESAR,SP_PELOTON,SP_TREX,SP_SHERWIN_WILLIAMS,SP_INSPIRED_CLOSETS,SP_SIMPLISAFE,SP_SLEEP_NUMBER", "source_code": "diy", "ip_address": "73.114.22.197", "mcmid": "35530902807482826999066543887443590483", "gender": "female", "email": "testemail@comcast.net", "action": "entry", "shows": "", "add_required_fields": ""}'
another_record = {"Data": data_2, "PartitionKey": unicode(partition_key, "utf-8")}
records.append(another_record)
batch_response = stream.put_records(Records=records, StreamName=stream_name)
print(batch_response)