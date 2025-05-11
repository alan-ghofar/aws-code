import json
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


# id menu cek table sc_02_menu
tableName = 'sl_03_m_grouping_price_list_item'


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

    try:
        sql = """
        SELECT
            gpl_item.id,
            gpl_item.gpl_id,
            gpl.description,
            gpl_item.item_code,
            ic.description item_name
        FROM
            `sl_03_m_grouping_price_list_item` gpl_item
            LEFT JOIN sl_02_m_grouping_price_list gpl ON ( gpl.id = gpl_item.gpl_id )
            LEFT JOIN in_01_m_item_codes ic ON ( ic.item_code = gpl_item.item_code ) 
        WHERE
            gpl_item.delete_mark = 0 
            AND gpl_item.active = 1
        """
    
        if params is not None:
            if params.get('id'):
                sql += " AND gpl_item.id = '" + params.get('id') + "'"
    
        cur.execute(sql)
        result = cur.fetchall()
    
        allData = []
        for row in result:
    
            data = {
                "id": row[0],
                "gpl_id": row[1],
                "description": row[2],
                "item_code": row[3],
                "item_name": row[4],
            }
            allData.append(data)
    except Exception as e:
        con.close()
        return inc_def.send_response_data(str(e), 500)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']
    
    field = {
        "gpl_id": req['gpl_id'],
        "item_code": req['item_code']
    }
    
    isDouble = inc_db.checkDoubleData(cur, field, tableName)
    
    if isDouble == "true" :
        return inc_def.send_response_data("Data is exist", 500)

    try:
        sqlInsert = """
        INSERT INTO `sl_03_m_grouping_price_list_item`
            (gpl_id, item_code, active, create_by, create_date, delete_mark)
        VALUES
            (%s, %s, %s, %s, %s, 0)
        """

        cur.execute(sqlInsert, (req['gpl_id'], req['item_code'],
                                req['active'], vToken['id_user'], datetime.date.today()))
        id = con.insert_id()

        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    sql = """
    SELECT
        gpl_item.id,
        gpl_item.gpl_id,
        gpl.description,
        gpl_item.item_code,
        ic.description item_name
    FROM
        `sl_03_m_grouping_price_list_item` gpl_item
        LEFT JOIN sl_02_m_grouping_price_list gpl ON ( gpl.id = gpl_item.gpl_id )
        LEFT JOIN in_01_m_item_codes ic ON ( ic.item_code = gpl_item.item_code ) 
    WHERE
        gpl_item.delete_mark = 0 
        AND gpl_item.active = 1
        AND gpl_item.id = %s
    """
    cur.execute(sql, (id))
    row = cur.fetchone()

    data = {
        "id": row[0],
        "gpl_id": row[1],
        "description": row[2],
        "item_code": row[3],
        "item_name": row[4],
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
    
    field = {
        "gpl_id": req['gpl_id'],
        "item_code": req['item_code']
    }
    
    isDouble = inc_db.checkDoubleData(cur, field, tableName)
    
    if isDouble == "true" :
        return inc_def.send_response_data("Data is exist", 500)

    try:
        sqlUpdate = """
        UPDATE `sl_03_m_grouping_price_list_item`
        SET
        	gpl_id = %s, 
            item_code = %s, 
            `active` = %s,
        	update_by = %s,
        	update_date = %s
        WHERE
        	id = %s
        """
        cur.execute(sqlUpdate, (req['gpl_id'], req['item_code'],
                                req['active'], vToken['id_user'], datetime.date.today(), id))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])

    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    sql = """
    SELECT
        gpl_item.id,
        gpl_item.gpl_id,
        gpl.description,
        gpl_item.item_code,
        ic.description item_name
    FROM
        `sl_03_m_grouping_price_list_item` gpl_item
        LEFT JOIN sl_02_m_grouping_price_list gpl ON ( gpl.id = gpl_item.gpl_id )
        LEFT JOIN in_01_m_item_codes ic ON ( ic.item_code = gpl_item.item_code ) 
    WHERE
        gpl_item.delete_mark = 0 
        AND gpl_item.active = 1
        AND gpl_item.id = %s
    """
    cur.execute(sql, (id))
    row = cur.fetchone()

    data = {
        "id": row[0],
        "gpl_id": row[1],
        "description": row[2],
        "item_code": row[3],
        "item_name": row[4],
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
	UPDATE `sl_03_m_grouping_price_list_item` 
	SET 
		delete_by = %s, 
		delete_date = %s, 
		delete_mark = 1 
	WHERE 
		id = %s
	"""
    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])

    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
