import json
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

tableName = 'in_01_m_material_issue_types'

def lambda_handler(event, context):
    #return inc_def.send_response_data(event, 200)
    
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
    dataMenu = inc_db.getDataMenu(con, idMenu)
    pathMenunya = dataMenu['path'] #inc_db.getPathMenu(con, idMenu)
    
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

    try:
        sql = "SELECT `id`, `mat_issue_type`, `coa` FROM `in_01_m_material_issue_types` WHERE `active` = 1 AND `delete_mark` = 0"
        if params is not None:
            if params.get('id'):
                sql += " AND id = '" + params.get('id') + "'"
    
        cur.execute(sql)
        myresult = cur.fetchall()
    
        allData = []
        for row in myresult:
            data = {
                "id": row[0],
                "mat_issue_type": row[1],
                "coa": row[2]
            }
            allData.append(data)
    except Exception as e:
        return inc_def.send_response_data(str(e), 500)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    sqlInsert = """
    INSERT INTO `in_01_m_material_issue_types` 
        ( `coa`, `mat_issue_type`, `active`, `create_by`, `create_date`, `delete_mark` ) 
    VALUES 
        (%s, %s, %s, %s, %s, 0)
    """
    cur.execute(sqlInsert, ( req['coa'], req['mat_issue_type'], req['active'], vToken['id_user'], datetime.date.today()))
    id = con.insert_id()
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    con.commit()
    

    sql = "SELECT id, mat_issue_type, coa FROM `in_01_m_material_issue_types` WHERE id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "mat_issue_type": row[1],
        "coa": row[2]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']
    
    id = params['id']
    sqlUpdate = """
    UPDATE `in_01_m_material_issue_types` 
    SET 
        `coa` = %s, 
        `mat_issue_type` = %s, 
        `update_by` = %s, 
        `update_date` = %s, 
        `active` = %s 
    WHERE 
       `id` = %s
    """
    cur.execute(sqlUpdate, (req['coa'], req['mat_issue_type'], vToken['id_user'], datetime.date.today(), req['active'], id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    con.commit()

    sql = "SELECT id, mat_issue_type, coa FROM `in_01_m_material_issue_types` WHERE id = %s"
    cur.execute(sql, (params['id']))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "mat_issue_type": row[1],
        "coa": row[2],
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    sqlUpdate = "UPDATE `in_01_m_material_issue_types` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE id = %s"
    cur.execute(sqlUpdate, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    con.commit()
    
    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
