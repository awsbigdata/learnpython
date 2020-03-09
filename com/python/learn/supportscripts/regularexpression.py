import re

email_address = '20/02/25 10:54:16 INFO SignalUtils: Registered signal handler for HUP'
match = re.search(r'(\d+/\d+/\d+\s+\d+:\d+:\d+)\s+([^\s]+).*', email_address)
for i in re.findall(r'(\d+/\d+/\d+\s+\d+:\d+:\d+)\s+([^\s]+).*', email_address):
    print i
if email_address:
  print(len(match.groups())) # The whole matched text
  print(match.group(1)) # The username (group 1)
  print(match.group(2)) # The host (group 2)

out=[i for i in match.groups()]
print(out)