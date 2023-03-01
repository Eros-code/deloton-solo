from flask import Flask, current_app, jsonify, request
import json
from json import load, dumps
from datetime import datetime
from dotenv import load_dotenv
load_dotenv(override=True)
import os
from sql_wrapper import SQLConnection
 
# Returns the current local date
current_time = datetime.now()

app = Flask(__name__)

dbname = os.environ["DBNAME"]
username = os.environ['SQL_USERNAME']
host = os.environ['SQL_HOST']
password = os.environ['SQL_PASSWORD']

sql = SQLConnection(dbname, username, host, password)

def load_rides():
    rides_df = sql.q("SELECT * FROM rides")
    rides_df['start_time'] = rides_df['start_time'].apply(lambda x: str(x))
    rides_df['start_year'] = rides_df['start_time'].apply(lambda x: str(x[:4]))
    rides_df['start_month'] = rides_df['start_time'].apply(lambda x: str(x[5:7]))
    rides_df['start_day'] = rides_df['start_time'].apply(lambda x: str(x[8:10]))
    return rides_df

users_df = sql.q("SELECT * FROM users")
users_df['account_created'] = users_df['account_created'].apply(lambda x: str(x))
joined_df = sql.q("""SELECT t1.gender, t1.age, t2.* FROM users AS t1
                    INNER JOIN rides AS t2 ON t1.user_id=t2.user_id""")

joined_df['start_time'] = joined_df['start_time'].apply(lambda x: str(x))

def load_male_rides():

    male_rides = sql.q("""SELECT t1.gender, t1.age, t2.* FROM users AS t1
                        INNER JOIN rides AS t2 ON t1.user_id=t2.user_id
                        WHERE t1.gender = 'male'""")

    male_rides['start_time'] = male_rides['start_time'].apply(lambda x: str(x))

    return male_rides

def load_female_rides():

    female_rides = sql.q("""SELECT t1.gender, t1.age, t2.* FROM users AS t1
                        INNER JOIN rides AS t2 ON t1.user_id=t2.user_id
                        WHERE t1.gender = 'female'""")

    female_rides['start_time'] = female_rides['start_time'].apply(lambda x: str(x))

    return female_rides

@app.route("/", methods=["GET"])
def test():
    return "Welcome to the Deloton API!"

# Get a ride with a specific ID:
@app.route("/ride/<int:ride_id>", methods=["GET"])
def get_user_ride(ride_id):
    rides_df = load_rides()
    df_filter = rides_df[rides_df["ride_id"] == ride_id]
    df_filter = df_filter.to_json(orient="records")
    return(df_filter)

# #Get riders information (e.g. name, gender, age, avg. heart rate, number of rides):
@app.route("/riders", methods=["GET"])
def get_users():
    df_filter = users_df
    df_filter = df_filter.to_json(orient="records")
    return(df_filter)

# #Get rider information (e.g. name, gender, age, avg. heart rate, number of rides):
@app.route("/rider/<int:user_id>", methods=["GET"])
def get_user(user_id):
    df_filter = users_df[users_df["user_id"] == user_id]
    df_filter = df_filter.to_json(orient="records")
    return((df_filter))

# #Get rider information by gender (e.g. name, gender, age, avg. heart rate, number of rides):
@app.route("/riders/<gender>", methods=["GET"])
def get_user_gender(gender):
    df_filter = users_df[users_df["gender"] == gender]
    df_filter = df_filter.to_json(orient="records")
    return((df_filter))

# #Get all rides:
@app.route("/rides", methods=["GET"])
def get_rides():
    rides_df = load_rides()
    return(rides_df.to_json(orient="records"))

# #Get rides by gender:
@app.route("/rides/<gender>", methods=["GET"])
def get_ride_gender(gender):
    male_rides = load_male_rides()
    female_rides = load_female_rides()

    if gender == "Male" or gender == "male":
        joined_rides = male_rides[["ride_id","start_time","duration","avg_resistance","avg_rpm","avg_power","avg_hrt", "user_id"]]
    else:
        joined_rides = female_rides[["ride_id","start_time","duration","avg_resistance","avg_rpm","avg_power","avg_hrt", "user_id"]]

    return(joined_rides.to_json(orient="records"))

# #Get rides by age or by an age range:
@app.route("/riders2", methods=["GET"])
def get_rider_age():
    args = request.args
    result = args.get('age', type=str)
    if '-' not in result:
      df_filter = users_df[users_df["age"] == int(result)]
      return(df_filter.to_json(orient="records"))
    else:
      result_arr = result.split("-")
      df_filter = users_df[(users_df["age"] >= int(result_arr[0])) & (users_df["age"] <= int(result_arr[1]))]
      return(df_filter.to_json(orient="records"))

# #Get all riders from specific location(s):
# @app.route("/rider", methods=["GET"])
# def locate_users():
#     args = request.args
#     result = args.get('location', type=str)

#     if ',' not in result:
#       df_filter = filter(lambda df_filter: df_filter["area"] == str(result), user_data)
#       return(dumps(list(df_filter)))
#     else:
#       result_arr = result.split(",")
#       location_list = []
#       for i in range(0, len(result_arr)):
#         df_filter = filter(lambda df_filter: df_filter["area"] == str(result_arr[i]), user_data)
#         location_list.append({str(result_arr[i]):list(df_filter)})
#     return(dumps(list(location_list)))

# ## Get all rides for a rider with a specific ID
@app.route("/rider/<int:user_id>/rides", methods=["GET"])
def get_user_rides(user_id):
    rides_df = load_rides()
    df_filter = rides_df[rides_df['user_id'] == user_id]
    return(df_filter.to_json(orient="records"))


# ## Get all rides for a specific date
@app.route("/daily", methods=["GET"])
def date():
    args = request.args
    result = args.get('date', type=str)
    current_year = current_time.year
    current_month = current_time.month
    current_day = current_time.day

    rides_df = load_rides()

    if result == None:
       current_year = current_time.year
       current_month = current_time.month
       current_day = current_time.day
       df_filter = rides_df[(rides_df["start_year"] == current_year) & (rides_df["start_month"] == current_month) & (rides_df["start_day"] == current_day)]
       return(df_filter.to_json(orient="records"))

    if '-' not in result:
        #### filter by year ####
        print(result)
        df_filter = rides_df[rides_df["start_year"] == result]
        return(df_filter.to_json(orient="records"))
    elif '-' in result:
        result_arr = result.split("-")

        ### filter by year and month ###
    if len(result_arr) == 2:
        df_filter = rides_df[(rides_df["start_year"] == result_arr[0]) & (rides_df["start_month"] == result_arr[1])]
        return(df_filter.to_json(orient="records"))
            
        ### filter by year, month and day
    if len(result_arr) > 2:
        df_filter = rides_df[(rides_df["start_year"] == result_arr[0]) & (rides_df["start_month"] == result_arr[1]) & (rides_df["start_day"] == result_arr[2])]
        return(df_filter.to_json(orient="records"))


# # # delete a ride with a specific ID:
@app.route("/ride/del/<int:session_id>", methods=["GET","DELETE"])
def delete_ride(session_id):
    sql.q("""DELETE FROM rides WHERE ride_id='%d'"""%(session_id))
    return('ride successfully deleted')
# 
#
  


#http://127.0.0.1:5000/search?tags=Europe
# GET /daily?date=01-01-2020 test
