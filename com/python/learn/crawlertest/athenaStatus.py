import boto3




def  extract_execution_status(numbeofrows=50):
    client = boto3.client('athena')

    response = client.list_query_executions(MaxResults=50);
    next=response['NextToken']
    remain_rows=numbeofrows-50
    while(remain_rows>50):

        remain_rows = numbeofrows - 50
    print(response)


extract_execution_status()


