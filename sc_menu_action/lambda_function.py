import json
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

tableName = 'sc_03_menu_action'

def lambda_handler(event, context):
    global idMenu
    httpMethod = event['httpMethod']
    headers = event['headers']
    user_token = headers.get('token')
    url_path = headers.get('path')
    
    pathActionnya = ''
    if httpMethod == 'GET':
        con = db_con.database_connection_read()
        pathActionnya = inc_def.getActionIndex()
    elif httpMethod == 'POST':
        con = db_con.database_connection_write()
        pathActionnya = inc_def.getActionCreate()
    elif httpMethod == 'PUT':
        con = db_con.database_connection_write()
        pathActionnya = inc_def.getActionEdit() + "/:id"
    elif httpMethod == 'DELETE':
        con = db_con.database_connection_write()
        pathActionnya = inc_def.getActionDelete() + "/:id"
    else:
        return inc_def.send_response_data([], 404)
    
    idMenu = inc_db.getIdMenuByTableName(con, tableName)
    
    pathMenunya = inc_db.getPathMenu(con, idMenu)

    pathFull = pathMenunya + "/" + pathActionnya
    
    if user_token is None:
        return inc_def.send_response_data("Access Denied", 403)
    else:
        vToken = valid_token.validationToken(user_token)
        if vToken == "false":
            return inc_def.send_response_data("Invalid Token", 404)
        else:
            vMenu = valid_menu.validationMenu(vToken['id_user'], url_path)
            if vMenu == "false":
                return inc_def.send_response_data("Forbidden access for this menu", 403)
            else:
                if url_path != pathFull:
                    return inc_def.send_response_data("Path menu action doesn't match", 403)
            event['data_user'] = vToken

    return getHttpMethod(con, httpMethod, event)


def getHttpMethod(con, httpMethod, event):
    if httpMethod == 'GET':
        return functionGet(con, event)
    elif httpMethod == 'POST':
        return functionPost(con, event)
    elif httpMethod == 'PUT':
        return functionPut(con, event)
    elif httpMethod == 'DELETE':
        return functionDelete(con, event)

# =============================== FUNCTION GET
def functionGet(con, event):
    cur = con.cursor()
    params = event['queryStringParameters']

    sql = """
    SELECT
        ma.id,
        ma.id_menu,
        m.nama_menu,
        ma.id_action,
        a.nama_action,
        m.path,
        ma.params 
    FROM
        `sc_03_menu_action` ma
        LEFT JOIN `sc_02_menu` m ON ( m.id = ma.id_menu )
        LEFT JOIN `sc_01_action` a ON ( a.id = ma.id_action ) 
    WHERE
        ma.`delete_mark` = 0 
        AND ma.`active` = 1
    """

    if params is not None:
        if params.get('id'):
            sql += " AND ma.id =" + params.get('id')
        if params.get('menu'):
            sql += " AND ma.id_menu =" + params.get('menu')

    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        menu_action = {
            "id": row[0],
            "id_menu": row[1],
            "nama_menu": row[2],
            "id_action": row[3],
            "nama_action": row[4],
            "params": row[6],
            "path": row[5] + "/" + row[4] if row[6] is None  else row[5] + "/" + row[4] + "/:" + row[6]

        }
        allData.append(menu_action)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    # return inc_def.send_response_data(req, 200)
    reqParams = None if req['params'] is None else req['params']
    sqlInsert = "INSERT INTO `sc_03_menu_action` ( id_menu, id_action, params, create_by, create_date, active, delete_mark ) VALUES (%s, %s, %s, %s, %s, 1, 0)"
    cur.execute(sqlInsert, (req['id_menu'], req['id_action'], reqParams, vToken['id_user'], datetime.date.today()))
    
    id = con.insert_id()
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    con.commit()
    
    sql = """
    SELECT
        ma.id,
        ma.id_menu,
        m.nama_menu,
        ma.id_action,
        a.nama_action,
        ma.params 
    FROM
        `sc_03_menu_action` ma
        LEFT JOIN `sc_02_menu` m ON ( m.id = ma.id_menu )
        LEFT JOIN `sc_01_action` a ON ( a.id = ma.id_action ) 
    WHERE
        ma.`delete_mark` = 0 
        AND ma.`active` = 1 
        AND ma.id =%s
    """
    cur.execute(sql, (id))
    row = cur.fetchone()
    menu_action = {
        "id": row[0],
        "id_menu": row[1],
        "nama_menu": row[2],
        "id_action": row[3],
        "nama_action": row[4],
        "params": row[5],
    }

    cur.close()
    return inc_def.send_response_data(menu_action, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']
    sqlUpdate = "UPDATE `sc_03_menu_action` SET id_menu=%s, id_action=%s, params=%s, update_by=%s, update_date=%s, active=%s WHERE id=%s"
    cur.execute(sqlUpdate, (req['id_menu'], req['id_action'], req['params'],
                vToken['id_user'], datetime.date.today(), req['active'], id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    con.commit()

    sql = """
    SELECT
        ma.id,
        ma.id_menu,
        m.nama_menu,
        ma.id_action,
        a.nama_action,
        ma.params 
    FROM
        `sc_03_menu_action` ma
        LEFT JOIN `sc_02_menu` m ON ( m.id = ma.id_menu )
        LEFT JOIN `sc_01_action` a ON ( a.id = ma.id_action ) 
    WHERE
        ma.`delete_mark` = 0 
        AND ma.id =%s
    """
    cur.execute(sql, (params['id']))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "id_menu": row[1],
        "nama_menu": row[2],
        "id_action": row[3],
        "nama_action": row[4]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']
    sqlUpdate = "UPDATE `sc_03_menu_action` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE id=%s"
    cur.execute(sqlUpdate, (vToken['id_user'],
                datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
