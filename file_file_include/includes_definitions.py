import json

#definisi action access table sc_01_action
#start
def getActionView():
    return 'view'

def getActionCreate():
    return 'create'

def getActionEdit():
    return 'edit'

def getActionDelete():
    return 'delete'

def getActionShow():
    return 'show'

def getActionIndex():
    return 'index'

def getActionBlocks():
    return 'blocks'
#end
#definisi action access table sc_01_action


# function for response
def send_response_data(data, status_code):
    response = {
        "status": status_code,
        "data": data
    }
    return {
        'statusCode': status_code,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*'
            # 'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(response, default=str)
    }