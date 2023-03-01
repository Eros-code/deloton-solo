import json
from confluent_kafka import Consumer
from dotenv import load_dotenv
import os
import uuid
import pandas as pd
import re 
import ast
import datetime
from sql_wrapper import SQLConnection

print("libraries imported and SQL connection script imported")

load_dotenv(override=True)
server = os.getenv("KAFKA_SERVER")
username = os.getenv("KAFKA_KEY")
password = os.getenv("KAFKA_SECRET")

today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days = 1)

### make a regex to match datetime in a string
regstr = re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')

### initialise date time of today
today = datetime.datetime.now()

def kafka_consumer() -> Consumer:
    """Makes a connection to a Kafka consumer"""
    c = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_SERVER"),
        "group.id": f"deloton + {uuid.uuid1()}",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.getenv("KAFKA_KEY"),
        "sasl.password": os.getenv("KAFKA_SECRET"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "False",
        "topic.metadata.refresh.interval.ms": "-1",
        "client.id": f"id-002-00e",
    })
    c.subscribe(["deloton"])
    return c

def get_msg(k):
    data = []
    print("attempting to retrieve data from kafka")
    count = 0
    while True:
        msg = k.poll(1.0)
        count += 1
        if msg is not None: #regstr.search(msg.value().decode('utf-8')[9:28]) is not None:
            data.append(json.loads(msg.value().decode('utf-8')))
            if regstr.search(msg.value().decode('utf-8')[9:28]) is not None:
                x = datetime.datetime.strptime(msg.value().decode('utf-8')[9:28], "%Y-%m-%d %H:%M:%S")
                if x >= today:
                    print("last message obtained")
                    break
        elif msg is None and count > 10:
            break
    k.commit()
    k.close()
    print("data retrieved")
    print("connection to kafka made")

    return data


def data_cleanser(data):
    users_rows = []
    rides_rows = []

    data_regex = re.compile(r'{[\s\S]*}')
    numbers_regex = re.compile(r'\d+\.?\d*')
    stop_words = ['Mr','Ms','Dr', 'Mrs', 'Miss']
    user = []
    start_datetime = ''
    start_time = ''
    duration = 0
    resistance = 0
    hrt = 0
    ride_id = -1
    rpm = 0
    power = 0
    existing_user = set()

    user_dict = {'user_id': None,
                'name': None,
                'gender': None,
                'date_of_birth': None,
                'bike_serial': None,
                'original_source': None,
                'height_cm': None,
                'weight_kg': None}

    for i in data:

        ## If a new ride starts all data is reset
        if '--------- beginning of a new ride' in i['log']:
            user = []
            start_datetime = 0
            duration = 0
            resistance = 0
            hrt = 0
            rpm = 0
            power = 0
            user_dict = {'user_id': None,
                'name': None,
                'gender': None,
                'date_of_birth': None,
                'bike_serial': None,
                'original_source': None,
                'height_cm': None,
                'weight_kg': None}
        
        else:
            ### Gets the start date/time of the ride
            if 'Getting user data from server' in i['log']:
                start_datetime = i['log'].split(' ')[0]
                start_year = start_datetime.split('-')[0]
                start_month = start_datetime.split('-')[1]
                start_day = start_datetime.split('-')[2]
                start_time = i['log'].split(' ')[1]
                ride_id += 1

            ## Gets the data of the user e.g name, address etc.
            if 'data = ' in i['log']:
                data = data_regex.findall(i['log'])
                user_dict = ast.literal_eval(data[0])
                age = datetime.datetime.now().year - datetime.datetime.fromtimestamp(user_dict['date_of_birth']/1000).year
                if user_dict['user_id'] not in existing_user:
                    existing_user.add(user_dict['user_id'])
                    name = user_dict['name'].split(' ')
                    if name[0] in stop_words:
                        name.pop(0)
                    address = user_dict['address'].split(',')
                    user_row = {'user_id':user_dict['user_id'], 'name': ' '.join(name), 'gender':user_dict['gender'], 'age':age, 'height':user_dict['height_cm'], 'weight':user_dict['weight_kg'], 'account_created':str(datetime.datetime.fromtimestamp(user_dict['account_create_date']/1000)), 'original_source':str(user_dict['original_source']), 'postcode':address[-1]}
                    users_rows.append(user_row)
            ## Gets the duration and resistance of the ride at every interval
            elif 'Ride - ' in i['log']:
                ride_data = numbers_regex.findall(i['log'])
                duration = ride_data[-2]
                resistance = ride_data[-1]
            
            ## telemetry data is required to calculate avg hrt, rpm and power
            elif 'Telemetry -' in i['log']:
                telemetry_data = numbers_regex.findall(i['log'])
                hrt = telemetry_data[-3]
                rpm = telemetry_data[-2]
                power = telemetry_data[-1]

            ## somehow we need to obtain the primary key from users table and put it into rides_rows as the foreign key
            rides_rows.append([start_datetime + ' ' + start_time, float(duration), int(resistance), float(rpm), float(power), int(hrt), user_dict['user_id']])

    print("data collected from kafka stream and stored")


    #### Here do a bulk insert of all users in users_rows into users table in sql

    ride_dict = {}
    for elem in rides_rows:
        if elem[-1] not in ride_dict:
            ride_dict[elem[-1]] = []
        ride_dict[elem[-1]].append(elem[:-1])

    for key in ride_dict:
        ride_dict[key] = [i for i in zip(*ride_dict[key])]

    sql_list = []
    for i in ride_dict.keys():
        sql_list.append({'start_time':ride_dict[i][0][1], 'duration':ride_dict[i][1][-1], 'avg_resistance':sum(ride_dict[i][2])/len(ride_dict[i][2]), 'avg_rpm':sum(ride_dict[i][3])/len(ride_dict[i][3]), 'avg_power':sum(ride_dict[i][4])/len(ride_dict[i][4]), 'avg_hrt':sum(ride_dict[i][5])/len(ride_dict[i][5]), 'user_id':i})

    print("data manipulation complete")

    sql_list = sql_list[1:-1]
    df = pd.DataFrame(sql_list)

    one_day_df = df

    one_day_dict = one_day_df.to_dict(orient="records")

    print("data reduction complete - last 24 hours of data obtained")

    return users_rows, one_day_dict

dbname = os.environ["DBNAME"]
username = os.environ['SQL_USERNAME']
host = os.environ['SQL_HOST']
password = os.environ['SQL_PASSWORD']

# cons = kafka_consumer()
# msg = get_msg(cons)
# clean_data = data_cleanser(msg)
# sql = SQLConnection(dbname, username, host, password)
# print("Connection to rds database successful")
# print("attempt to batch insert into sql tables")
# sql.batch_insert(clean_data[0], "users")
# sql.batch_insert(clean_data[1], "rides")

def lambda_handler(event, context):
    cons = kafka_consumer()
    msg = get_msg(cons)
    clean_data = data_cleanser(msg)
    sql = SQLConnection(dbname, username, host, password)
    print("Connection to rds database successful")
    print("attempt to batch insert into sql tables")
    sql.batch_insert(clean_data[0], "users")
    sql.batch_insert(clean_data[1], "rides")
    return {"statusCode": 200}
