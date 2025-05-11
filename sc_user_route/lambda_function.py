# import pymysql
import json
import datetime
import valid_token
import valid_menu
import database_connection


def lambda_handler(event, context):
    # TODO implement
    # return {
    #     'statusCode': 200,
    #     'headers': {
    #         'Access-Control-Allow-Headers': '*',
    #         'Access-Control-Allow-Origin': '*'
    #         # 'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
    #     },
    #     'body': json.dumps(event)
    # }

    httpMethod = event['httpMethod']
    headers = event['headers']
    user_token = headers.get('token')
    # url_path = headers.get('path')

    if user_token is None:
        return send_response("Access Denied", 403)
    else:
        vToken = valid_token.validationToken(user_token)
        if vToken == "false":
            return send_response("Invalid Token", 404)
        else:
            # vMenu = valid_menu.validationMenu(vToken['id_user'], url_path)
            # if vMenu == "false":
            #     return send_response("Forbidden access for this menu", 403)
            event['data_user'] = vToken

    return getHttpMethod(httpMethod, event)


def getHttpMethod(httpMethod, event):
    if httpMethod == 'GET':
        con = database_connection.database_connection_read()
        return functionGet(con, event)

    else:
        return send_response([], 404)


# function for response
def send_response(data, status_code):
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
        'body': json.dumps(response)
    }


# =============================== FUNCTION GET
def functionGet(con, event):
    cur = con.cursor()
    params = event['queryStringParameters']
    vToken = event['data_user']

    sql = """
    SELECT
    	ma.id,
	    m.keterangan,
    	m.path,
    	a.nama_action,
    	ma.params 
    FROM
    	`sc_02_user_resp` ur
    	LEFT JOIN `sc_04_responsibility_item` ri ON ( ri.id_resp = ur.id_responsibility )
    	LEFT JOIN `sc_02_menu` m ON ( m.id = ri.id_menu )
    	LEFT JOIN `sc_03_menu_action` ma ON ( ma.id = ri.id_menu_action )
    	LEFT JOIN `sc_01_action` a ON ( a.id = ma.id_action ) 
    WHERE
    	ur.id_user = %s
    """
    if params is not None:
        if params.get('id_perusahaan'):
            sql += " AND ur.kode_perusahaan = '" + params.get('id_perusahaan') + "'"
            
    sql += """
    	AND ur.delete_mark = 0 
        AND ri.delete_mark = 0 
        AND m.delete_mark = 0 
        AND ma.delete_mark = 0 
        AND a.delete_mark = 0
        AND ri.active = 1 
        AND m.active = 1 
        AND ma.active = 1 
        AND a.active = 1
    """

    cur.execute(sql, (vToken['id_user']))
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        menuStr = row[3] + row[2]
        path = "/" + row[2] + "/" + row[3] if row[4] is None else "/" + \
            row[2] + "/" + row[3] + "/:" + row[4]

        action = {
            "key": row[0],
            "path": path,
            "filename": menuStr
        }
        allData.append(action)

    cur.close()
    return send_response(allData, 200)
