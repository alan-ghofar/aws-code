import json
import datetime
import valid_token as v_token
import valid_menu as v_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db
import dateutil.tz


#code menu cek table sc_02_menu
tableName = 'sl_01_m_credit_status'


def lambda_handler(event, context):
    global idMenu
    global tz_JKT
    
    tz_JKT = dateutil.tz.gettz('Asia/Jakarta')
    
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
        vToken = v_token.validationToken(user_token)
        if vToken == "false":
            return inc_def.send_response_data("Invalid Token", 404)
        else:
            vMenu = v_menu.validationMenu(vToken['id_user'], url_path)
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

    sql = "SELECT id, reason_description, dissallow_invoices FROM `sl_01_m_credit_status` WHERE delete_mark = 0 AND active = 1"
    
    if params is not None:
        if params.get('id'):
            sql += " AND id LIKE '%" + params.get('id') + "%'"

    cur.execute(sql)
    result = cur.fetchall()

    allData = []
    for row in result:
        data = {
            "id": row[0],
            "reason_description": row[1],
            "dissallow_invoices": row[2]
        }
        allData.append(data)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    sqlInsert = """
    INSERT INTO `sl_01_m_credit_status`
        (reason_description, dissallow_invoices, active, create_by, create_date, delete_mark)
    VALUES
        (%s, %s, %s, %s, %s, 0)
    """

    try:
        cur.execute(sqlInsert, (req['reason_description'], req['dissallow_invoices'], 
            req['active'], vToken['id_user'], datetime.datetime.now(tz_JKT)))
        id = con.insert_id()
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
        con.commit()
        
    
        sql = "SELECT id, reason_description, dissallow_invoices FROM `sl_01_m_credit_status` WHERE id = %s"
        cur.execute(sql, (id))
        row = cur.fetchone()
        data = {
            "id": row[0],
            "reason_description": row[1],
            "dissallow_invoices": row[2]
        }
    
        cur.close()
        return inc_def.send_response_data(data, 200)
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    

# =============================== FUNCTION PUT
def functionPut(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    
    try:
        sqlUpdate = """
        UPDATE `sl_01_m_credit_status`
        SET
            reason_description = %s,
            dissallow_invoices = %s,
            active = %s,
            update_by = %s,
            update_date = %s
        WHERE
            id = %s
        """
        
        cur.execute(sqlUpdate, (req['reason_description'], req['dissallow_invoices'], 
            req['active'], vToken['id_user'], datetime.datetime.now(tz_JKT), id))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur.close()
    return inc_def.send_response_data("Updated!", 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    
    try:
        sqlDelete = """
        UPDATE `sl_01_m_credit_status` 
        SET 
            delete_by = %s, 
            delete_date = %s, 
            delete_mark = 1 
        WHERE 
            id = %s
        """
        
        cur.execute(sqlDelete, (vToken['id_user'], datetime.datetime.now(tz_JKT), id))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur.close()
    return inc_def.send_response_data("Deleted!", 200)