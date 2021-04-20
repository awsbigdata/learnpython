import datetime
import sys
import boto3.session


tmp_access_key="ASIAWLJ4FSEHTZ62WHHU";
tmp_secret_key="jNDWbGCG6sAHJxK1EKVrR5m8OoubM+qF3JHQ4qfj";
security_token="""IQoJb3JpZ2luX2VjENn//////////wEaCXVzLWVhc3QtMSJIMEYCIQCSP978Ul26QfOtz13MNtrV3wZBfhwBUDlMVu0KgzVRSQIhANpUeQ0D2cpBYme9EI+2vob+zuFPbVdr1OkS0YKmaTmiKqYCCIL//////////wEQAhoMNDM2NjAyMjQ1MzkxIgzQtn6kizWlt9udYTgq+gEPyxYVgvCqURERFpk8wVETvuyUpVl2AnMkcwk6N+5SR4+ksFuvGL4T5+IIcWTfb7JwBTz+1XKvVP9AfrSHhS4XP/mIp+xaJWnc3ygzjQTFlzhzeeAcvtIx2aNahZhpsIzpWPPzrvbrQ2q/RHzQiJbvTFZsBovAUTWhxhHFhNnYtJFqYePtnFZMnORrRy0b1Kf5iTQ1Z4hRj5qNthm6Ln8clGwZ7xzCrQfGI341kfE84fVyqfQWjc6Funz7n38jxVRk5HIGSoZZuCLMRK8NWpZJTkJCjBWOY81GHat9wXRStMw/la5fdAc/cdi5lO2hcXM9dI20wjfFG8tkMNHY//4FOpwBO9MJCraKVW1tDN+ADDTXPXQnAUtmOKquu9V7RAej39h8DCIDmQDtrqYOW5VrewMSp7czG1N0kuXZdp75PZB7WLsvp1hpO31d6QxU1nnhd8TlEku2AWZFI+x5JXc9mtazFT2wIR8x7lF/9lEkAIAhB69wICEe2LQCuTr37POEuLn2DGsAV+xyQ2v0H1g5MINQc7VMqf36N7tYEptZ""";
boto3_session = boto3.session.Session(
    aws_access_key_id=tmp_access_key,
    aws_secret_access_key=tmp_secret_key,
    aws_session_token=security_token
)

client = boto3_session.client('logs',region_name='ap-southeast-2')

response = client.describe_log_groups(
)
print(response)
def isoTomillSecondConvert(stime):
    date = datetime.datetime.strptime(stime, '%Y-%m-%dT%H:%M:%S')
    timestamp = str((date - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)
    return int(timestamp[:-2])

stream_response = client.describe_log_streams(
    logGroupName="docker_logs.log",  # Can be dynamic
    orderBy='LastEventTime',  # For the latest events
    limit=1 # the last latest event, if you just want one
)

print(stream_response)

starttime=isoTomillSecondConvert('2020-12-15T16:00:00')
endtime=isoTomillSecondConvert('2020-12-18T22:30:01')

print(starttime)
#latestlogStreamName = stream_response["logStreams"]["logStreamName"]

logGroupName='docker_logs.log'
logStreamName = 'i-09e1ca624d9c81e56 # Defaults to ec2 instance id'
marker = None
file = open('/Users/srramas/Downloads/glue_log.txt', 'w')
while True:
    if marker:
        print("reading next")
        response_iterator = response = client.get_log_events(
         logGroupName=logGroupName,
         logStreamName=logStreamName,
         nextToken=marker,
         startFromHead=True

        )
    else:
        response_iterator = response = client.get_log_events(
            logGroupName=logGroupName,
            logStreamName=logStreamName,
           # startTime=starttime,
           # endTime=endtime,
           startFromHead = True
        )
    for event in response_iterator['events']:
       # print()
        file.write(event['message'])
    try:
        if(marker !=response_iterator['nextForwardToken']):
            marker = response_iterator['nextForwardToken']
            #print(marker)
        else:
            print("completed")
            file.close()
            sys.exit(0)
    except KeyError:
       print("error")
       sys.exit(0)