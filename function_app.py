import azure.functions as func
import logging
from services.data_consumer import get_data_from_storage_account
from services.data_consumer import get_most_recent_data_from_storage_account
from utils.create_http_response import create_http_response
from utils.error.error_mapper import INVALID_REQUEST_ERROR, UNKNOWN_ERROR
import datetime

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="consumption")
def consumption(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('[SCRIPT INFO] -- Python HTTP trigger function processed a request.')
    
    try:
        json_data = req.get_json()
    except Exception as e:
        logging.error(f'[error]: {e}')
        create_http_response(INVALID_REQUEST_ERROR, 415)
        
    try:
        year = json_data['year']
        month = json_data['month']
        day = json_data['day']
    except Exception as e:
        today = datetime.datetime.now()
        year = int(today.year)
        month = int(today.month)
        day = int(today.day)
        
    try:
        data_history = get_data_from_storage_account(year=year, month=month, day=day)
        if not data_history:
            return create_http_response(UNKNOWN_ERROR, 500)
        return create_http_response(data_history, 200)
    except Exception as e:
        logging.error(f'[error]: {e}')
        return create_http_response(UNKNOWN_ERROR, 500)

@app.route(route="most_recent_consumption")
def most_recent_consumption(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('[SCRIPT INFO] -- Python HTTP trigger function processed a request.')
    
    try:
        most_recent_data = get_most_recent_data_from_storage_account()
        if not most_recent_data:
            return create_http_response(UNKNOWN_ERROR, 500)
        return create_http_response(most_recent_data, 200)
    except Exception as e:
        logging.error(f'[error]: {e}')
        return create_http_response(UNKNOWN_ERROR, 500)
        