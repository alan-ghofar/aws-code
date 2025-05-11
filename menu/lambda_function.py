import json
import datetime
import pymysql.cursors
import valid_token as v_token
import valid_menu as v_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

#id menu cek table sc_02_menu
tableName = "sc_02_menu"


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
        vToken = v_token.validationToken(user_token)
        if vToken == "false":
            return inc_def.send_response_data("Invalid Token", 404)
        else:
            vMenu = v_menu.validationMenu(vToken['id_user'], url_path)
            if vMenu == "false" and httpMethod != 'GET':
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
    cur = con.cursor(pymysql.cursors.DictCursor)
    params = event['queryStringParameters']

    sql = """
    SELECT
        m.id,
        m.nama_menu,
        m.path,
        m.keterangan,
        m.priority,
        m.id_menu_category,
        mc.nama_category,
        mc.keterangan keterangan_category,
        m.gcode,
        m.parent_id,
        m.code_doc
    FROM
        `sc_02_menu` m
        LEFT JOIN `sc_01_menu_category` mc ON ( mc.id = m.id_menu_category ) 
    WHERE
        m.`delete_mark` = 0 
        AND m.`active` = 1
    """

    if params is not None:
        if params.get('id'):
            sql += " AND m.id =" + params.get('id')
        if params.get('category'):
            sql += " AND m.id_menu_category = " + params.get('category')

    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:

        sql_action = """
        SELECT
        	ma.id,
        	ma.id_action,
        	a.nama_action 
        FROM
        	`sc_03_menu_action` ma
        	LEFT JOIN `sc_01_action` a ON ( a.id = ma.id_action ) 
        WHERE
        	ma.`delete_mark` = 0 
        	AND ma.`active` = 1 
        	AND ma.id_menu = %s
        """
        cur.execute(sql_action, (row["id"]))
        res_action = cur.fetchall()

        all_action = []
        for row_act in res_action:
            action = {
                "id_menu_action": row_act["id"],
                "id_action": row_act["id_action"],
                "nama_action": row_act["nama_action"],
                "path_action": row["path"] + "/" + row_act["nama_action"]
            }
            all_action.append(action)

        menu = {
            "id": row["id"],
            "gcode": row["gcode"],
            "code_doc": row["code_doc"],
            "nama_menu": row["nama_menu"],
            "path": row["path"],
            "keterangan": row["keterangan"],
            "priority": row["priority"],
            "id_menu_category": row["id_menu_category"],
            "nama_category": row["nama_category"],
            "keterangan_category": row["keterangan_category"],
            "parent_id": row['parent_id'],
            "action": all_action
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
        "gcode": req['gcode'],
        "path": req['path'],
    }
    
    isDouble = inc_db.checkDoubleData(cur, field, tableName)
    
    if isDouble == "true" :
        return inc_def.send_response_data("GCode or Path is exist", 500)

    try:
        sqlInsert = """
        INSERT INTO `sc_02_menu` 
            ( gcode, code_doc, nama_menu, path, id_menu_category, keterangan, priority, parent_id, create_by, create_date, active, delete_mark ) 
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, 0)
        """
        cur.execute(sqlInsert, (req['gcode'], req['code_doc'], req['nama_menu'], req['path'], req['id_menu_category'],
                    req['keterangan'], req['priority'], req['parent_id'], vToken['id_user'], datetime.date.today()))
        id = con.insert_id()
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur.close()
    return inc_def.send_response_data("Inserted!", 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']
    idEdit = params['id']

    cekData = validationDataDouble(event, cur)
    if cekData == 1:
        return inc_def.send_response_data({"message": "Nama menu telah digunakan!"}, 500)

    try:
        sqlUpdate = """
        UPDATE `sc_02_menu` 
        SET 
            gcode = %s, 
            code_doc = %s, 
            nama_menu = %s, 
            path = %s, 
            id_menu_category = %s, 
            keterangan = %s, 
            priority = %s, 
            parent_id = %s, 
            update_by = %s, 
            update_date = %s, 
            active = %s 
        WHERE 
            id = %s"""
        cur.execute(sqlUpdate, (req['gcode'], req['code_doc'], req['nama_menu'], req['path'], req['id_menu_category'], req['keterangan'],
                    req['priority'], req['parent_id'], vToken['id_user'], datetime.date.today(), req['active'], idEdit))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), idEdit, '', tableName, vToken['id_user'])
        con.commit()

    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur.close()
    return inc_def.send_response_data("Updated", 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    idDelete = params['id']

    sqlUpdate = "UPDATE `sc_02_menu` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE id=%s"
    cur.execute(sqlUpdate, (vToken['id_user'],
                datetime.date.today(), "1", idDelete))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), idDelete, '', tableName, vToken['id_user'])
    con.commit()

    cur.close()
    return inc_def.send_response_data("Deleted", 200)


# =============================== FUNCTION VALIDATION DATA DOUBLE
def validationDataDouble(event, cur):
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']

    sql = "SELECT COUNT(path) AS cek FROM `sc_02_menu` WHERE delete_mark = 0 AND path = %s"

    if params is not None:
        if params.get('id'):
            sql += " AND id <> " + params.get('id')

    cur.execute(sql, (req['path']))
    row = cur.fetchone()

    if row[0] != 0:
        return 1

    return 0
