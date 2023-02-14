
# Snowflake External Function - Flood Monitoring Station Readings

## Sends a request to the API for UK flood monitoring
## stations to retrieve the latest reading

## Import Azure modules
import azure.functions as func
import logging

## Import necessary modules for the function
import json
import http.client
import mimetypes

## Define the function which retrieves the latest readings for a single station ID
def retrieve_latest_reading(station_id: str) :
  ### Provide root URL for the connection to access
  url = "environment.data.gov.uk"

  ### Build an HTTP connection to the above URL
  conn = http.client.HTTPConnection(url)

  ### Since our API does not require one, we use an empty payload
  payload = ""

  ### To make our lives easier, we instruct the API to return a JSON object
  headers = {"Accept": "application/json"}

  ### Define the actual endpoint on the API that we wish to access
  ### Note that we specify the stationId in this endpoint
  endpoint = f"/flood-monitoring/id/stations/{station_id}/measures"

  ### Access the endpoint with a "GET" request, converting the response to a JSON object
  conn.request("GET", endpoint, payload, headers)
  res = conn.getresponse()
  data = res.read()
  json_response =  json.loads(data.decode("utf-8"))

  ### Return the JSON Response object as the output of our function
  return json_response

## Create the function to retrieve all latest readings from an incoming
## Snowflake-structured array of data
def retrieve_and_parse_latest_readings(station_id: str) :

  ### Execute the function to send a request to the
  ### API to retrieve the latest readings for a given station
  latest_reading_json = retrieve_latest_reading(station_id=station_id)

  ### Retrieve the datetime and value for the latest reading from latestReadingJSON
  latest_reading_datetime = latest_reading_json["items"][0]["latestReading"]["dateTime"]
  latest_reading_value = latest_reading_json["items"][0]["latestReading"]["value"]

  ### Return the combined output
  parsed_reading_json = {"datetime": latest_reading_datetime, "value" : latest_reading_value}
  return parsed_reading_json

## Create the function to retrieve all latest readings from an incoming
## Snowflake-structured array of data
def retrieve_all_latest_readings(station_array_list: list) :
  ### Iterate over input rows to
  ### perform the row-level function and
  ### store the results in a single list
  ### that is a compatible response for
  ### a Snowflake External Function.
  response_list = []
  for station_array in station_array_list :

    ### Retrieve the row number
    row_number = station_array[0]
    
    ### Retrieve the station ID
    station_id = station_array[1]

    ### try/except is used for error handling
    try:
      #### Execute the function to retrieve and parse the reading
      parsed_reading_json = retrieve_and_parse_latest_readings(station_id=station_id)
    except:
      parsed_reading_json = "Error"
    
    ### Append the result to the list
    ### of rows to return
    response_list.append([row_number, parsed_reading_json])

  return response_list

## Define the main function
def main(req: func.HttpRequest) -> func.HttpResponse:

  ### Attempt to parse the body of the request as JSON
  try: 
    req_body = req.get_json()
  except ValueError: 
    logging.info("Failed to parse request body as JSON")
    logging.info(e)
    return func.HttpResponse(body="Failed to parse request body as JSON", status_code=400)
  else: 

    try :

      #### Additional logging that is not necessary
      #### once in production
      logging.info("req_body:")
      logging.info(req_body)

      #### Retrieve the "data" key of the request body.
      #### When the request comes from Snowflake, we expect data to be a
      #### an array of each row in the Snowflake query resultset.
      #### Each row is its own array, which begins with the row number,
      #### for example [0, 690250] would be the 0th row, in which the
      #### variable value passed to the function is 690250.
      input_rows = req_body.get("data") 

      #### Additional logging that is not necessary
      #### once in production
      logging.info("input_rows:")
      logging.info(input_rows)

      #### Iterate over input rows to
      #### perform the row-level function and
      #### store the results in a single list
      #### that is a compatible response for
      #### a Snowflake External Function.
      response_list = retrieve_all_latest_readings(input_rows)

      #### Put response into a JSON dictionary,
      #### then convert it to a string for transmission
      response_json = {"data": response_list}
      response_as_string = json.dumps(response_json)

      #### Send the response
      response_headers = {"Content-Type" : "application/json"}
      return  func.HttpResponse(body=response_as_string, status_code=200, headers=response_headers)

    ### Error handling
    except Exception as e:
      
      logging.info(f"Manual log - Error encountered")
      logging.info(e)
      return  func.HttpResponse(body=f"Error encountered", status_code=400)
