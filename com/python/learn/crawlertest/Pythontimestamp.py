import datetime

def breakoutdate(rec):
    #ts = datetime.fromtimestamp(rec)
    return rec.day

a = datetime.datetime(2017, 11, 8,10,11,12)
b=a+datetime.timedelta(days=2)
print(breakoutdate(b))