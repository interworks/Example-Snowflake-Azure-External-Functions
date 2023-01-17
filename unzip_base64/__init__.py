
## Import Azure modules
import azure.functions as func
import logging

## Import necessary modules for the function
import json
import base64
import io
import zipfile

## Define row-level function to unzip the base64-encoded string
def unzip_base64_encoded_string(base64_encoded_string: str) :

  ### try/except is used for error handling
  try:
    #### Use base64 module to decode the string
    decoded_string = base64.b64decode(base64_encoded_string)

    #### try/except is used for error handling
    try:
      ##### Unzip the file (converting the string to bytes in the process)
      with zipfile.ZipFile(io.BytesIO(decoded_string)) as zf:
        
        ###### If the zip only contains a single file
        ###### then read it, otherwise return a generic
        ###### startement regarding multiple files
        if len(zf.namelist()) > 1 :
          retrieved_data = "The zipfile contained multiple files"
        else :
          ####### Loop through each file in the zipfile (should only
          ####### be one) and write it to the data variable
          for name in zf.namelist():
            with zf.open(name) as f:
              retrieved_data = f.read().decode()

      return retrieved_data

    except:
      ##### Return a bespoke error message
      return f"String was not a singular zipped text file: {decoded_string}"

  except:
    #### Return a bespoke error message
    return f"String was not base64 utf-8 encoded: \"{decoded_string}\""

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
      #### for example [0, "my_encoded_string"] would be the 0th row, in which the
      #### variable passed to the function is "my_encoded_string".
      input_rows = req_body.get("data") 

      #### Additional logging that is not necessary
      #### once in production
      logging.info("input_rows:")
      logging.info(input_rows)

      #### Iterate over input rows to
      #### perform the row-level function and
      #### store the results in a single list
      #### that is a compatible response for
      #### a Snowflake External Function
      response_list = []
      for input_row in input_rows :

        ##### Retrieve the row number
        row_number = input_row[0]

        ##### Retrieve the function input
        ##### and execute the function
        base64_encoded_string = input_row[1]
        decoded_file_contents = unzip_base64_encoded_string(base64_encoded_string=base64_encoded_string)
        
        ##### Append the result to the list
        ##### of rows to return
        response_list.append([row_number, decoded_file_contents])

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
