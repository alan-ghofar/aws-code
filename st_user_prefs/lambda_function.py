import json
import datetime
import pymysql.cursors
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

tableName = "st_02_user_prefs"

def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 403)
    
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
            # if vMenu == "false" and httpMethod != 'GET':
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
    vToken = event['data_user']
    
    cur = con.cursor()
    params = event['queryStringParameters']

    sql = """
    SELECT
        up.id,
        up.kode,
        up.nilai,
        up.keterangan
    FROM
        `st_02_user_prefs`  up
        LEFT JOIN `st_01_sys_prefs` sp ON (sp.kode = up.kode)
    WHERE
        up.active = 1 
        AND up.delete_mark = 0
    """
    if params is not None:
        if params.get('id'):
            sql += " AND up.id = '" + params.get('id') + "'"
    
    sql += " AND up.id_user = '" + vToken['id_user'] + "'"
    
    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        data = {
            "id": row[0],
            "kode": row[1],
            "nilai": row[2],
            "keterangan": row[3]
        }
        allData.append(data)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    sqlInsert = """
    INSERT INTO `st_02_user_prefs` 
    ( `id_user`, `kode`, `nilai`, `keterangan`, `create_by`, `create_date`, `active`, `delete_mark` ) 
    VALUES 
    (%s, %s, %s, %s, %s, %s, %s, 0)
    """
    cur.execute(sqlInsert, (vToken['id_user'], req['kode'], req['nilai'],
                req['keterangan'], vToken['id_user'], datetime.date.today(), req['active']))
    id = con.insert_id()
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    con.commit()

    
    sql = """
    SELECT
        kode,
        nilai,
        keterangan
    FROM
        `st_02_user_prefs` 
    WHERE
        active = 1 
        AND delete_mark = 0
        AND id = %s
    """
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "kode": row[0],
        "nilai": row[1],
        "keterangan": row[2]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']
    
    banyakData = len(req)
    dataKe = 0
    

    for row in req:
        sql = "UPDATE `st_02_user_prefs` SET "
        sql += " `update_by` = '" + str(vToken['id_user']) + "', "
        sql += " `update_date` = '" + str(datetime.date.today()) + "', "
        sql += " `kode` = '" + str(row['kode']) + "', "
        sql += " `nilai` = '" + str(row['nilai']) + "' "

        sql += " WHERE id = " + str(row['id'])
        
        cur.execute(sql)
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), row['id'], '', tableName, vToken['id_user'])
        con.commit()
        
    sql = """
    SELECT
        up.id,
        up.kode,
        up.nilai,
        up.keterangan
    FROM
        `st_02_user_prefs`  up
        LEFT JOIN `st_01_sys_prefs` sp ON (sp.kode = up.kode)
    WHERE
        up.active = 1 
        AND up.delete_mark = 0
    """
    sql += " AND up.id_user = '" + vToken['id_user'] + "'"
    
    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        data = {
            "id": row["id"],
            "kode": row["kode"],
            "nilai": row["nilai"],
            "keterangan": row["keterangan"]
        }
        allData.append(data)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    sqlUpdate = "UPDATE `st_02_user_prefs` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE id=%s"
    cur.execute(sqlUpdate, (vToken['id_user'],
                datetime.date.today(), "1", params['id']))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
