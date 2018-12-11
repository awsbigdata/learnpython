import re
import sys

prog = re.compile('([a-zA-Z]{3} \d{2} \d{2}:\d{2}:\d{2}) ([a-zA-Z0-9-]*) (haproxy\[\d+\]:) \d+.\d+.\d+.\d+.:\d+ (\[\d{2}\/[a-zA-Z]{3}\/\d{4}:\d{2}:\d{2}:\d{2}.\d{3}\]) ([a-zA-Z0-9_-]+) ([a-zA-Z0-9_-]+)\/([<>a-zA-Z0-9_-]+) (-1|\d+)\/(-1|\d+)\/(-1|\d+)\/(-1|\d+)\/(\+?\d+) (\d{3}).*')

##haproxy.log-02-1518949021

if  len(sys.argv)<2:
    print("please pass the input file name")
    sys.exit(1)

def  validate(row):
      return prog.match(row)


f=open(sys.argv[1],'r')

#validate regex
for line in f:
    if not validate(str(line)):
        print(line)
