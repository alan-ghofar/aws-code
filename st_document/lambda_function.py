import json
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


# id menu cek table sc_02_menu
tableName = 'st_01_document'


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
        `code_doc`,
        `type_doc`,
        `description`,
        `mark`,
        `recount`,
        `digit_number`,
        `prefix`,
        `separator`
    FROM
        `st_01_document` 
    WHERE
        delete_mark = 0 
        AND active = 1
    """

    if params is not None:
        if params.get('id'):
            sql += " AND code_doc = '" + params.get('id') + "'"

    cur.execute(sql)
    result = cur.fetchall()

    allData = []
    for row in result:

        data = {
            "code_doc": row[0],
            "type_doc": row[1],
            "description": row[2],
            "mark": row[3],
            "recount": row[4],
            "digit_number": row[5],
            "prefix": row[6],
            "separator": row[7],
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
        "code_doc": req['code_doc'],
    }
    
    isDouble = inc_db.checkDoubleData(cur, field, tableName)
    
    if isDouble == "true" :
        return inc_def.send_response_data("Data is exist", 500)

    try:
        sqlInsert = """
        INSERT INTO `st_01_document`
            (code_doc, type_doc, description, mark, recount, digit_number, prefix, `separator`, `active`, create_by, create_date, delete_mark)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
        """

        cur.execute(sqlInsert, (req['code_doc'], req['type_doc'], req['description'], req['mark'], 
                                req['recount'], req['digit_number'], req['prefix'], req['separator'],
                                req['active'], vToken['id_user'], datetime.date.today()))
        id = req['code_doc']

        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    sql = sql = """
    SELECT
        `code_doc`,
        `type_doc`,
        `description`,
        `mark`,
        `recount`,
        `digit_number`,
        `prefix`,
        `separator`
    FROM
        `st_01_document` 
    WHERE
        `code_doc` = %s
    """
    cur.execute(sql, (id))
    row = cur.fetchone()

    data = {
        "code_doc": row[0],
        "type_doc": row[1],
        "description": row[2],
        "mark": row[3],
        "recount": row[4],
        "digit_number": row[5],
        "prefix": row[6],
        "separator": row[7],
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

    try:
        sqlUpdate = """
        UPDATE `st_01_document`
        SET
        	code_doc = %s, 
            type_doc = %s, 
            description = %s, 
            mark = %s, 
            recount = %s, 
            digit_number = %s, 
            prefix = %s, 
            `separator` = %s, 
            `active` = %s,
        	update_by = %s,
        	update_date = %s
        WHERE
        	code_doc = %s
        """
        cur.execute(sqlUpdate, (req['code_doc'], req['type_doc'], req['description'], req['mark'], 
                                req['recount'], req['digit_number'], req['prefix'], req['separator'],
                                req['active'], vToken['id_user'], datetime.date.today(), id))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])

    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    sql = sql = """
    SELECT
        `code_doc`,
        `type_doc`,
        `description`,
        `mark`,
        `recount`,
        `digit_number`,
        `prefix`,
        `separator`
    FROM
        `st_01_document` 
    WHERE
        `code_doc` = %s
    """
    cur.execute(sql, (req['code_doc']))
    row = cur.fetchone()

    data = {
        "code_doc": row[0],
        "type_doc": row[1],
        "description": row[2],
        "mark": row[3],
        "recount": row[4],
        "digit_number": row[5],
        "prefix": row[6],
        "separator": row[7],
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']

    sqlDelete = """
	UPDATE `st_01_document` 
	SET 
		delete_by = %s, 
		delete_date = %s, 
		delete_mark = 1 
	WHERE 
		code_doc = %s
	"""
    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])

    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
