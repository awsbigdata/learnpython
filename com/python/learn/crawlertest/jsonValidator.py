import json

def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError, e:
    return False
  return True


input_file="/home/local/ANT/srramas/Downloads/testspace.json"

##Iterate the job
with open(input_file) as inp:
    line = inp.readline()
    while line:
        if not (is_json(line)):
            print(line)
        line=inp.readline()