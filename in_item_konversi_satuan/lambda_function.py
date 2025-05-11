import json
import pymysql.cursors
import datetime
import hashlib
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db
    
tableName = 'in_04_m_item_konversi_satuan'

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
    cur = con.cursor(pymysql.cursors.DictCursor)
    params = event['queryStringParameters']

    sql = """
    SELECT
    	konv.id,
    	konv.item_code,
    	stk.description,
    	konv.unit_code,
    	un.`name` unit_desc,
    	konv.konversi,
    	konv.reverse_konversi
    FROM
    	in_04_m_item_konversi_satuan konv
    	LEFT JOIN in_03_m_stock_master stk 
    	ON ( konv.item_code = stk.stock_code ) 
    	LEFT JOIN in_01_m_item_units un
    	ON (un.`code` = konv.unit_code)
    WHERE
    	konv.active = 1 
    	AND konv.delete_mark = 0
    """
    if params is not None:
        if params.get('id'):
            sql += " AND konv.id =" + params.get('id')
        if params.get('item_code'):
            sql += " AND konv.item_code = '" + params.get('item_code')+"'"
    
    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        menu = {
            "id": row['id'],
            "item_code": row['item_code'],
            "description": row['description'],
            "unit_code": row['unit_code'],
            "unit_desc": row['unit_desc'],
            "konversi": row['konversi'],
            "reverse_konversi": row['reverse_konversi']
        }
        allData.append(menu)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    sqlInsert = """
    INSERT INTO `in_04_m_item_konversi_satuan` 
        ( `item_code`, `unit_code`, `konversi`, `reverse_konversi`, `active`, `create_by`, `create_date`, `delete_mark` ) 
    VALUES 
        (%s, %s, %s, %s, %s, %s, %s, 0)
    """
    cur.execute(sqlInsert, (req['item_code'],  req['unit_code'],  req['konversi'],  req['reverse_konversi'], req['active'], vToken['id_user'], datetime.date.today()))
    id = con.insert_id()
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    con.commit()
    

    sql = "SELECT id, item_code, unit_code, konversi, reverse_konversi FROM `in_04_m_item_konversi_satuan` WHERE id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "item_code": row[1],
        "unit_code": row[2],
        "konversi": row[3],
        "reverse_konversi": row[4]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']

    sqlUpdate = "UPDATE `in_04_m_item_konversi_satuan` SET  item_code=%s, unit_code=%s, konversi=%s, reverse_konversi=%s, update_by=%s, update_date=%s, active=%s WHERE id=%s"
    cur.execute(sqlUpdate, (req['item_code'], req['unit_code'], req['konversi'],
                req['reverse_konversi'], vToken['id_user'], datetime.date.today(), req['active'], id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    con.commit()
    sql = "SELECT * FROM `in_04_m_item_konversi_satuan` WHERE id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "item_code": row[1],
        "unit_code": row[2],
        "konversi": row[3],
        "reverse_konversi": row[4]
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

    sqlUpdate = "UPDATE `in_04_m_item_konversi_satuan` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE id=%s"
    cur.execute(sqlUpdate, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    data = {
        "message": "Deleted!"
    }
    con.commit()
    cur.close()
    return inc_def.send_response_data(data, 200)
