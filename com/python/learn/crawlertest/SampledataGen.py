
import random
import string
import os
import datetime
import json



def random_generator(size=6, chars=string.ascii_uppercase + string.digits):
   return ''.join(random.choice(chars) for x in range(size))

def randon_double(digits=4):
    rounded_number = round(random.uniform(100000, 1000000), digits)
    return rounded_number

def create_file(numberofrows=10000):
    indate = 1
    date= datetime.datetime(1980, 11, 8, 10, 11, 12)

    output_file = createOutputDir(date)
    output = open(output_file, "w")
    for i in range(0,numberofrows) :
        my_dictionary={}
        my_dictionary["id"] =i%10
        my_dictionary["price"] = randon_double()
        my_dictionary["product"] = "glue"
        output.write(json.dumps(my_dictionary))
        output.write('\n')
        if i % 50==0:
            path_dt=date+datetime.timedelta(minutes=indate)
            indate+=1;
            output.close()
            print(path_dt)
            output_file=createOutputDir(path_dt)
            output = open(output_file, "w")
            #print("find the csv here")
            #print(output_file)

def createOutputDir(date):
    output_path = "/tmp/test/output/year={0}/month={1}/day={2}/hour={3}/".format(date.year, date.month, date.day,date.hour,date.minute)
    output_file = output_path + "output.csv"
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    return output_file


#print(round(random.uniform(100000, 1000000),3))
create_file(numberofrows=200);
print("completed")