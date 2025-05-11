import json
import datetime
import pymysql.cursors
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


# id menu cek table sc_02_menu
tableName = 'st_02_document_status'


def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 404)
    
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
        d.code_doc,
        d.description,
        d.mark,
        ds.type_doc,
        ds.status_code,
        ds.status_name,
        ds.memo
    FROM
        `st_02_document_status` ds
        LEFT JOIN st_01_document d ON ( d.type_doc = ds.type_doc AND d.delete_mark = 0 AND d.active = 1) 
    WHERE
        ds.delete_mark = 0 
        AND ds.active = 1
    """

    if params is not None:
        if params.get('id'):
            sql += " AND ds.type_doc = '" + params.get('id') + "'"
    
        if params.get('status_code'):
            sql += " AND ds.status_code = '" + params.get('status_code') + "'"

    cur.execute(sql)
    result = cur.fetchall()

    allData = []
    for row in result:

        data = {
            "code_doc": row['code_doc'],
            "description": row['description'],
            "type_doc": row['type_doc'],
            "status_code": row['status_code'],
            "status_name": row['status_name'],
            "memo": row['memo'],
            "mark": row['mark'],
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

    field = {
        "type_doc": req['type_doc'],
        "status_code": req['status_code'],
    }
    
    isDouble = inc_db.checkDoubleData(cur, field, tableName)
    
    if isDouble == "true" :
        return inc_def.send_response_data("Data is exist", 500)

    try:
        sqlInsert = """
        INSERT INTO `st_02_document_status`
            (type_doc, status_code, status_name, memo, `active`, create_by, create_date, delete_mark)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, 0)
        """

        cur.execute(sqlInsert, (req['type_doc'], req['status_code'], req['status_name'], req['memo'],
                                req['active'], vToken['id_user'], datetime.date.today()))
        id = req['type_doc']

        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
    SELECT
        d.code_doc,
        d.description,
        d.mark,
        ds.type_doc,
        ds.status_code,
        ds.status_name,
        ds.memo
    FROM
        `st_02_document_status` ds
        LEFT JOIN st_01_document d ON ( d.type_doc = ds.type_doc AND d.delete_mark = 0 AND d.active = 1) 
    WHERE
        ds.delete_mark = 0 
        AND ds.active = 1
        AND ds.`type_doc` = %s
        AND ds.status_code = %s
    """
    cur.execute(sql, (id, req['status_code']))
    row = cur.fetchone()

    data = {
        "code_doc": row['code_doc'],
        "description": row['description'],
        "type_doc": row['type_doc'],
        "status_code": row['status_code'],
        "status_name": row['status_name'],
        "memo": row['memo'],
        "mark": row['mark'],
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    status = params['status']

    try:
        sqlUpdate = """
        UPDATE `st_02_document_status`
        SET
            status_name = %s, 
            memo = %s, 
            `active` = %s,
        	update_by = %s,
        	update_date = %s
        WHERE
        	type_doc = %s
        	AND status_code = %s
        """
        cur.execute(sqlUpdate, (req['status_name'], req['memo'], 
                                req['active'], vToken['id_user'], datetime.date.today(), id, status))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])

    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
    SELECT
        d.code_doc,
        d.description,
        d.mark,
        ds.type_doc,
        ds.status_code,
        ds.status_name,
        ds.memo
    FROM
        `st_02_document_status` ds
        LEFT JOIN st_01_document d ON ( d.type_doc = ds.type_doc AND d.delete_mark = 0 AND d.active = 1) 
    WHERE
        ds.delete_mark = 0 
        AND ds.active = 1
        AND ds.`type_doc` = %s
        AND ds.status_code = %s
    """
    cur.execute(sql, (id, status))
    row = cur.fetchone()

    data = {
        "code_doc": row['code_doc'],
        "description": row['description'],
        "type_doc": row['type_doc'],
        "status_code": row['status_code'],
        "status_name": row['status_name'],
        "memo": row['memo'],
        "mark": row['mark'],
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    status = params['status']

    sqlDelete = """
	UPDATE `st_02_document_status` 
	SET 
		delete_by = %s, 
		delete_date = %s, 
		delete_mark = 1 
	WHERE 
		type_doc = %s
		AND `status_code` = %s
	"""
    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), id, status))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])

    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
