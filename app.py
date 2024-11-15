# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt
from datetime import datetime
from dateutil.relativedelta import relativedelta

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, select
from sqlalchemy import inspect

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Create our session (link) from Python to the DB
session = Session(engine)

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Functions
#################################################
def get_last_date():
    # get the dfte of the last row of data
    latest_date_row = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    latest_date = latest_date_row[0]
    latest_date = datetime.strptime(latest_date, '%Y-%m-%d').date()
    return latest_date
    
def get_from_date():
    # get latest date and date for one year previous to current
    latest_date_row = get_last_date()
    from_date = latest_date - relativedelta(years=1)
    return from_date

def get_all_dates():
    #get list of all aailable DB dates
    all_dates = session.query(Measurement.date).all()
    all_dates_list =[i[0] for i in all_dates]
    return all_dates_list

def check_for_date_in_DB(date_to_check):
    
    #get list of all available DB dates
    all_dates_list = get_all_dates()
    #print(all_dates_list)
    print(f"date to check: {date_to_check}")
    print(f"type of date to check {type(date_to_check)}")
    print(f"date sample: {all_dates_list[1]}")
    print(f"type of date sample {type(all_dates_list[1])}")
    date_to_check = str(date_to_check)
    if date_to_check in all_dates_list:
        print(f"found date {date_to_check}")
        return True
    else:
        print(f"did not find date {date_to_check}")
        return False
        
#################################################
# Flask Routes
#################################################
@app.route("/")
def welome():

    return (
        f"Welome to the Hawaii weather station API<br/>"
        f"These endpoints are available:<br/>"
        f"+++++++++++++++++++++++++++++<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"<br/>"
        f"Fetch summary weather info for a range of dates:<br/>"
        f" -- from a date to last DB date:<br/>"
        f"/api/v1.0/2015-05-03<br/>"
        f"   > bad date for error test: /api/v1.0/2024-01-09<br/>"
        f" -- for a range of dates:<br/>"
        f"/api/v1.0/2015-01-09/2017-01-03<br/>"
        f"   > bad date for error test: /api/v1.0/2004-01-09/2016-04-04<br/>"
        f"   > bad date for error test:/api/v1.0/2016-01-10/2024-04-03<br/>"
        f"   > bad date for error test:/api/v1.0/2016-01-10/2014-04-03<br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    
    from_date = get_from_date()
    
    # get date and precipitation for last year of data
    last_yr_prcp = session.query(Measurement.date, Measurement.prcp).filter\
        (Measurement.date >= from_date).all()

    # close session
    session.close()

    #list date, prcp for jsonification
    prcp_list =[]
    for date, prcp in last_yr_prcp:
        prcp_dict = {}
        prcp_dict["date"] = date
        prcp_dict["prcp"] = prcp
        prcp_list.append(prcp_dict)
 
    return jsonify(prcp_list)

@app.route("/api/v1.0/stations")
def stations(): 

    # get list of stations 
    stations = session.query(Station.station).all()

    # convert lsit of tuples to plain list
    station_list =[i[0] for i in stations]
    
    # close session
    session.close()

    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
# Query the dates and temperature observations of the most-active station for the previous year of data
# Return a JSON list of temperature observations for the previous year

    # find most active station
    observations_by_station = session.query(Measurement.station, func.count(Measurement.station))\
        .group_by(Measurement.station).all()
    observations_by_station_sorted = sorted(observations_by_station,\
        key=lambda tupl: tupl[1], reverse=True)
    most_active_station = observations_by_station_sorted[0][0] 

    # get from_date to retrieve last year of data
    from_date = from_date = get_from_date()

    # get temperaure readings for last year of data for most active station
    most_active_station_temp_data = session.query(Measurement.tobs)\
        .filter(Measurement.station == most_active_station).all()

    #convert to plain list
    temp_list = [i[0] for i in most_active_station_temp_data]
    
    # close session
    session.close()

    ##

    return jsonify(temp_list)
          
@app.route("/api/v1.0/<start_date>")
def start_to_last(start_date):
    end_date = get_last_date()

    if not check_for_date_in_DB(start_date):
        return jsonify ({"error": f"start_date {start_date} not found in DB"}), 404
    
    start_date = datetime.strptime(start_date, '%Y-%m-%d')

    # process request
    # TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date 
    # and less than or equal to the end date

    temps = session.query(Measurement.tobs).filter\
        (Measurement.date >= start_date).all()
    
    temps_list =[i[0] for i in temps]

    temp_dict = {
        "temp":temps_list
    }
    temps_df = pd.DataFrame(temp_dict)
    df = temps_df.describe()
    print(df)
    mean_temp = df.iloc[1]['temp']
    min_temp = df.iloc[3]['temp']
    max_temp = df.iloc[7]['temp']

    mean_temp = round(mean_temp, 2)
    
    temperature_summary = {
        'mean temperature': mean_temp,
        'max temperature': max_temp,
        'min temperature': min_temp
    }

    return jsonify(temperature_summary)  
    session.close() 

@app.route("/api/v1.0/<start_date>/<end_date>")
def start_and_end(start_date, end_date):
    print(f"start date:{start_date}")
    print(f"end date:{end_date}")
    if (not check_for_date_in_DB(start_date)) and (not check_for_date_in_DB(end_date)):
        return jsonify ({"error": f"start_date {start_date} and end_date{end_date} not found in DB"}), 404

    elif not check_for_date_in_DB(start_date):
        return jsonify ({"error": f"start_date {start_date} not found in DB"}), 404
        
    elif not check_for_date_in_DB(end_date):
        return jsonify ({"error": f"end_date {end_date} not found in DB"}), 404

    elif end_date < start_date:
        return jsonify ({"error": f"end_date {end_date} is earlier than start_date{start_date}"}), 404
    
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # process request
    # TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date 
    # and less than or equal to the end date

    temps = session.query(Measurement.tobs).filter\
        (Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    
    temps_list =[i[0] for i in temps]

    temp_dict = {
        "temp":temps_list
    }
    temps_df = pd.DataFrame(temp_dict)
    df = temps_df.describe()
    print(df)
    mean_temp = df.iloc[1]['temp']
    min_temp = df.iloc[3]['temp']
    max_temp = df.iloc[7]['temp']

    mean_temp = round(mean_temp, 2)
    
    temperature_summary = {
        'mean temperature': mean_temp,
        'max temperature': max_temp,
        'min temperature': min_temp
    }

    return jsonify(temperature_summary)  
    session.close()  

if __name__ == '__main__':
    app.run(debug=True)