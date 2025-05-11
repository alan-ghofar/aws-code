import json
import datetime
import hashlib
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db
    
tableName = 'pr_01_m_type_beli_supplier'

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
        type_beli_id,
        type_beli_name
    FROM
        `pr_01_m_type_beli_supplier` 
    WHERE
        delete_mark = 0 
        AND active = 1
    """
    if params is not None:
        if params.get('id'):
            sql += " AND type_beli_id =" + params.get('id')

    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        menu = {
            "type_beli_id": row[0],
            "type_beli_name": row[1],
            # "create_by": row[2],
            # "create_date": row[3],
            # "update_by": row[4],
            # "update_date": row[5],
            # "delete_by": row[6],
            # "delete_date": row[7],
            # "active": row[8],
            # "delete_mark": row[9],

        }
        allData.append(menu)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    field = {
        "type_beli_name": req['type_beli_name']
    }

    isDouble = inc_db.checkDoubleData(cur, field, tableName)

    if isDouble == "true" :
        return inc_def.send_response_data("Data is exist", 400)

    try:

        sqlInsert = "INSERT INTO `pr_01_m_type_beli_supplier` (  type_beli_name, create_by, create_date, active, delete_mark) VALUES (%s, %s, %s, 1, 0)"
        cur.execute(sqlInsert, (req['type_beli_name'], vToken['id_user'], datetime.date.today()))
        id = con.insert_id()
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response-data(str(e), 500)
    else:
        con.commit()

    sql = "SELECT * FROM `pr_01_m_type_beli_supplier` WHERE type_beli_id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "type_beli_id": row[0],
        "type_beli_name": row[1]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
    
# =============================== FUNCTION PUT
def functionPut(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']

    try:

        sqlUpdate = "UPDATE `pr_01_m_type_beli_supplier` SET  type_beli_name=%s, update_by=%s, update_date=%s, active=%s WHERE type_beli_id=%s"
        cur.execute(sqlUpdate, (req['type_beli_name'], vToken['id_user'], datetime.date.today(), req['active'], id))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()
        
    sql = "SELECT * FROM `pr_01_m_type_beli_supplier` WHERE type_beli_id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "type_beli_id": row[0],
        "type_beli_name": row[1],
        "active": row[8]
    }
    
    cur.close()
    return inc_def.send_response_data(data, 200)
    
# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    #req = json.loads(event['body'])
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']

    sqlUpdate = "UPDATE `pr_01_m_type_beli_supplier` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE type_beli_id=%s"
    cur.execute(sqlUpdate, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    data = {
        "message": "Deleted!"
    }
    con.commit()
    cur.close()
    return inc_def.send_response_data(data, 200)
