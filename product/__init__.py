
# Snowflake External Function - Product

## Simply adds together the input variables
## which are expected as numbers

## Import Azure modules
import azure.functions as func
import logging

## Import necessary modules for the function
import json
import functools

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
      #### for example [0, 2, 3] would be the 0th row, in which the
      #### variables passed to the function are 2 and 3. In this case,
      #### the function product output would be 6.
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
        
        ##### Retrieve the array of numbers to product
        numbers_to_multiply = input_row[1:]

        ##### try/except is used for error handling
        try:
          ###### Calculate the row product
          row_product = functools.reduce(lambda a,b : a*b, numbers_to_multiply)
        except:
          row_product = "Error"
        
        ##### Append the result to the list
        ##### of rows to return
        response_list.append([row_number, row_product])

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
