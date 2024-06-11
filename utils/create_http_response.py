import azure.functions as func
import json

def create_http_response(body: dict, status_code: int) -> dict:
    return func.HttpResponse(
             body=json.dumps(body),
             status_code=status_code,
             mimetype='application/json'
        )