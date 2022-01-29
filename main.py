import requests
import json
import paho.mqtt.client as mqtt
import time

HELP_TEXT = '''Look at shorturl.at/akyES for some docs!'''

APIKEY = '8112c86d6d4fca7a4ab5fed299a7d259'
MQTT_SERVER = 'ec2-100-27-23-78.compute-1.amazonaws.com'
MQTT_PORT = 1883

API_PARAM={'appid':APIKEY}
WEATHER_ENDPOINT = 'https://api.openweathermap.org/data/2.5/weather'
AIR_POLLUTION_ENDPOINT = 'http://api.openweathermap.org/data/2.5/air_pollution' # Solo funciona por coordenadas
GEOCODING_ENDPOINT = 'http://api.openweathermap.org/geo/1.0/' # (Ciudad, estado de EEUU y codigo de pais, separados por coma Y [numero de localizaciones máx. 5]) o (codigo postal y codigo de pais)
ONECALL_ENDPOINT = 'https://api.openweathermap.org/data/2.5/onecall'
LAST_ONETIME_SENT = int(time.time()) # segs desde desde 1/1/1970 00:00:00
LAST_MIN_NORMAL_CHECK = int(time.time() / 60) # mins desde 1/1/1970 00:00:00
MADE_NORMAL_CALLS = 0

client = mqtt.Client(client_id="WeatherAPIID", 
                     clean_session=True,
                     userdata=None, 
                     protocol=mqtt.MQTTv311, 
                     transport="tcp")

def pushMsg(topic, payload):
    res = {"res": str(payload)}
    client.publish(topic, json.dumps(res),
                   qos=0, retain=False)

def on_connect(client, userdata, flags, rc):
    print("Connected to ", client._host, "port: ", client._port)
    print("Flags: ", flags, "returned code: ", rc)

    client.subscribe("reqs/+", qos=0)
    client.subscribe("help", qos=0)

def makeCurrentWeatherCall(msg, topic):

    params = dict(API_PARAM)
    reqtype = msg["type"]

    global MADE_NORMAL_CALLS

    try:
        resmode = msg["mode"]

        if (resmode != "JSON" and resmode != "xml" and resmode != "html"):
            pushMsg(topic+"/resp", "mode not one of JSON, XML or HTML")
            return
    except:
        resmode = "JSON"

    params["mode"]=resmode

    try:
        resunits = msg["units"]

        if (resunits != "standard" and resunits != "metric" and resunits != "imperial"):
            pushMsg(topic+"/resp", "units not one of standard, metric or imperial")
            return
    except:
        resunits = "standard"

    params["units"]=resunits

    try:
        reslang = msg["lang"]
    except:
        reslang = "en"

    params["lang"]=reslang

    if (reqtype == "city"):

        try:
            cityname_param = msg["city_name"]
            try:
                statecode = msg["state_code"]
                cityname_param = cityname_param + "," + statecode
                try:
                    countrycode = msg["country_code"]
                    cityname_param = cityname_param + "," + countrycode
                    print("City name AND state_code AND county_code")
                except:
                    print("City name AND state_code")
            except:
                print("Only city name")
        except:
            pushMsg(topic+"/resp", "At least 'city_name' must be present for a City name request")
            return

        params["q"]=cityname_param
        respuesta = requests.get(WEATHER_ENDPOINT, params=params)

        print(respuesta.content)
        pushMsg(topic+"/resp",respuesta.content)     
    elif (reqtype == "id"):
        try:
            params["id"] = msg["city_id"]
        except:
            pushMsg(topic+"/resp", "'city_id' needed for a city ID request")
            return

        respuesta = requests.get(WEATHER_ENDPOINT, params=params)

        print(respuesta.content)
        print(respuesta.url)
        pushMsg(topic+"/resp",respuesta.content)
    elif (reqtype == "geo"):
        try:
            params["lat"] = msg["lat"]
            params["lon"] = msg["lon"]
        except:
            pushMsg(topic+"/resp", "'lat' AND 'lon' must be present for a geo coord request")
            return
            
        respuesta = requests.get(WEATHER_ENDPOINT, params=params)

        print(respuesta.content)
        print(respuesta.url)
        pushMsg(topic+"/resp",respuesta.content)
    elif (reqtype == "zip"):
        zipparam = None
        try:
            zipparam = msg["zip"]
        except:
            pushMsg(topic+"/resp", "'zip' code must be present for a zip code request")
            return

        try:
            countrycode = msg["country_code"]
            zipparam = zipparam + "," + countrycode
        except:
            pushMsg(topic+"/resp", "'countrycode' code must be present for a zip code request")
            return
            
        params['zip']=zipparam
        respuesta = requests.get(WEATHER_ENDPOINT, params=params)

        print(respuesta.content)
        print(respuesta.url)
        pushMsg(topic+"/resp",respuesta.content)
    else:
        pushMsg(topic+"/resp","The selected request type is not valid for a current weather request")
        return
    
    MADE_NORMAL_CALLS += 1

def makeAirPollutionCall(msg, topic):
    
    global MADE_NORMAL_CALLS
    
    params = dict(API_PARAM)
    reqtype = msg["type"]

    if (reqtype == "current"):
        try:
            params["lat"] = msg["lat"]
            params["lon"] = msg["lon"]
        except:
            pushMsg(topic+"/resp", "'lat' AND 'lon' must be present for an Air Pollution Request")
            return
            
        respuesta = requests.get(AIR_POLLUTION_ENDPOINT, params=params)

        print(respuesta.content)
        print(respuesta.url)
        pushMsg(topic+"/resp",respuesta.content)
    elif (reqtype == "forecast"):
        try:
            params["lat"] = msg["lat"]
            params["lon"] = msg["lon"]
        except:
            pushMsg(topic+"/resp", "'lat' AND 'lon' must be present for an Air Pollution Forecast Request")
            return
            
        respuesta = requests.get(AIR_POLLUTION_ENDPOINT+"/forecast", params=params)

        print(respuesta.content)
        print(respuesta.url)
        pushMsg(topic+"/resp",respuesta.content)
    elif (reqtype == "history"):
        try:
            params["lat"] = msg["lat"]
            params["lon"] = msg["lon"]
        except:
            pushMsg(topic+"/resp", "'lat' AND 'lon' must be present for an Air Pollution Historical Request")
            return
        
        try:
            params["start"] = int(msg["start"])
            params["end"] = int(msg["end"])
        except:
            pushMsg(topic+"/resp", "'start' AND 'end' must be present AND an integer for an Air Pollution Historical Request")
            return
        
        respuesta = requests.get(AIR_POLLUTION_ENDPOINT+"/history", params=params)

        print(respuesta.content)
        print(respuesta.url)
        pushMsg(topic+"/resp",respuesta.content)
    else:
        pushMsg(topic+"/resp","The selected request type is not valid for an Air pollution request")
        return

    MADE_NORMAL_CALLS += 1

def makeGeocodingCall(msg, topic):

    global MADE_NORMAL_CALLS

    params = dict(API_PARAM)
    reqtype = msg["type"]
    limit = 1
    try:
        limit = msg["limit"]
    except:
        print("Can't parse the limit parameter, setting a 1 default")

    if (reqtype == "reverse"):
        try:
            params["lat"] = msg["lat"]
            params["lon"] = msg["lon"]
        except:
            pushMsg(topic+"/resp", "'lat' AND 'lon' must be present for a Reverse Geocoding Request")
            return
        
        params["limit"] = limit
        respuesta = requests.get(GEOCODING_ENDPOINT+"reverse", params=params)

        print(respuesta.content)
        print(respuesta.url)
        pushMsg(topic+"/resp",respuesta.content)
    elif (reqtype == "direct_name"):
        try:
            cityname_param = msg["city_name"]
            try:
                statecode = msg["state_code"]
                cityname_param = cityname_param + "," + statecode
                try:
                    countrycode = msg["country_code"]
                    cityname_param = cityname_param + "," + countrycode
                    print("City name AND state_code AND county_code")
                except:
                    print("City name AND state_code")
            except:
                print("Only city name")
        except:
            pushMsg(topic+"/resp", "At least 'city_name' must be present for a Direct Geocoding Name Request")
            return

        params["q"]=cityname_param
        params["limit"] = limit
        respuesta = requests.get(GEOCODING_ENDPOINT+"/direct", params=params)

        print(respuesta.content)
        print(respuesta.url)
        pushMsg(topic+"/resp",respuesta.content)
    elif (reqtype == "direct_zip"):
        zipparam = None
        try:
            zipparam = msg["zip"]
        except:
            pushMsg(topic+"/resp", "'zip' code must be present for a zip code request")
            return

        try:
            countrycode = msg["country_code"]
            zipparam = zipparam + "," + countrycode
        except:
            pushMsg(topic+"/resp", "'countrycode' code must be present for a zip code request")
            
        params['zip']=zipparam
        respuesta = requests.get(GEOCODING_ENDPOINT+"/zip", params=params)

        print(respuesta.content)
        print(respuesta.url)
        pushMsg(topic+"/resp",respuesta.content)
    else:
        pushMsg(topic+"/resp","The selected request type is not valid for an Air pollution request")
        return

    MADE_NORMAL_CALLS += 1

def makeOneTimeCall(msg, topic):

    global LAST_ONETIME_SENT

    params = dict(API_PARAM)
    reqtype = msg["type"]

    try:
        resunits = msg["units"]

        if (resunits != "standard" and resunits != "metric" and resunits != "imperial"):
            pushMsg(topic+"/resp", "units not one of standard, metric or imperial")
            return
    except:
        resunits = "standard"

    params["units"]=resunits

    try:
        reslang = msg["lang"]
    except:
        reslang = "en"

    params["lang"]=reslang

    if(reqtype == 'non_historical'):
        try:
            params["lat"] = msg["lat"]
            params["lon"] = msg["lon"]
        except:
            pushMsg(topic+"/resp", "'lat' AND 'lon' must be present for a One Call request")
            return

        to_exclude = None
        try:
            string_list = msg['exclude']
            excluded_list = list(dict.fromkeys(string_list.split(',')))
            to_exclude = ','.join(excluded_list)
            params['exclude']=to_exclude
        except:
            print("No hay excluidos, o no se han podido interpretar bien")

        respuesta = requests.get(ONECALL_ENDPOINT, params=params)

        print(respuesta.content)
        print(respuesta.url)
        pushMsg(topic+"/resp",respuesta.content)
    elif (reqtype == 'historical'):
        try:
            params["lat"] = msg["lat"]
            params["lon"] = msg["lon"]
        except:
            pushMsg(topic+"/resp", "'lat' AND 'lon' must be present for a One Call request")
            return

        try:
            params["dt"] = msg["dt"]
            currSecs = int(time.time())

            if ((currSecs - int(params["dt"])) > 432000):
                pushMsg(topic+"/resp", "'dt' must be from the last 5 days")
                return
        except:
            pushMsg(topic+"/resp", "'dt' must be present for a One Call request ")
            return

        to_exclude = None
        try:
            string_list = msg['exclude']
            excluded_list = list(dict.fromkeys(string_list.split(',')))
            to_exclude = ','.join(excluded_list)
            params['exclude']=to_exclude
        except:
            print("No hay excluidos, o no se han podido interpretar bien")

        respuesta = requests.get(ONECALL_ENDPOINT+"/timemachine", params=params)

        print(respuesta.content)
        print(respuesta.url)
        pushMsg(topic+"/resp",respuesta.content)

    else:
        pushMsg(topic+"/resp","The selected request type is not valid for a One Call Request")
        return
    
    LAST_ONETIME_SENT = int(time.time())

def unrecognizedCall(topic):
    pushMsg(topic+"/resp", HELP_TEXT)


def parse_req(msg):
    try:
        themsg = json.loads(msg.payload.decode("utf-8"))
    except:
        pushMsg(msg.topic+"/resp", "Could not parse JSON request, check the payload, then, try again!")

    currTime = int(time.time())
    CallType = themsg["CallType"]

    global LAST_MIN_NORMAL_CHECK
    global LAST_ONETIME_SENT
    global MADE_NORMAL_CALLS

    if(CallType == "CurrentWeather"):        
        if(int(currTime / 60) > LAST_MIN_NORMAL_CHECK):
            MADE_NORMAL_CALLS = 0
            LAST_MIN_NORMAL_CHECK = currTime / 60

        if(MADE_NORMAL_CALLS < 23):
            makeCurrentWeatherCall(themsg, msg.topic)      # 23 CALLS / MIN
        else:
            pushMsg(msg.topic + "/resp","API CALLS LIMIT REACHED FOR THIS MINUTE. TRY AGAIN LATER!")
    elif (CallType == "OneTime"):

        if((currTime - LAST_ONETIME_SENT) >= 90):
            makeOneTimeCall(themsg, msg.topic)   # OneTime APROX 1 EVERY 1.5 MIN
        else:
            pushMsg(msg.topic + "/resp","ONETIME API CALL LIMIT REACHED THIS 90 s PERIOD. TRY AGAIN LATER!")
    elif (CallType == "AirPollution"):
        if(int(currTime / 60) > LAST_MIN_NORMAL_CHECK):
            MADE_NORMAL_CALLS = 0
            LAST_MIN_NORMAL_CHECK = currTime / 60

        if(MADE_NORMAL_CALLS < 23):
            makeAirPollutionCall(themsg, msg.topic)        # 23 CALLS / MIN
        else:
            pushMsg(msg.topic + "/resp","API CALLS LIMIT REACHED FOR THIS MINUTE. TRY AGAIN LATER!")
    elif (CallType == "Geocoding"):
        if(int(currTime / 60) > LAST_MIN_NORMAL_CHECK):
            MADE_NORMAL_CALLS = 0
            LAST_MIN_NORMAL_CHECK = currTime / 60

        if(MADE_NORMAL_CALLS < 23):
            makeGeocodingCall(themsg, msg.topic)           # 23 CALLS / MIN
        else:
            pushMsg(msg.topic + "/resp","API CALLS LIMIT REACHED FOR THIS MINUTE. TRY AGAIN LATER!")
    else:
        unrecognizedCall(msg.topic)

def public_help_message():
    pushMsg("help", HELP_TEXT)


def on_message(client, userdata, msg):
    if (msg.topic == "help"):
        public_help_message()
    else:
        parse_req(msg)
    
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set("WeatherAPIID", password=None)
client.connect(MQTT_SERVER, port=MQTT_PORT, keepalive=60)
client.loop_forever()