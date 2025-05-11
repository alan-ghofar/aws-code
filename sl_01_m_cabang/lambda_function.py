import json
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

#id menu cek table sc_02_menu
def get_id_menu():
    return '34'

def master_table_name():
    return 'sl_01_m_cabang'

def lambda_handler(event, context):
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
    
    pathMenunya = inc_db.getPathMenu(con, get_id_menu())

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

	sql = "SELECT cabang_code, description FROM `sl_01_m_cabang` WHERE `active`= 1 AND delete_mark = 0"
	if params is not None:
		if params.get('id'):
			sql += " AND cabang_code LIKE '%" + params.get('id') + "%'"

	cur.execute(sql)
	result = cur.fetchall()

	allData = []
	for row in result:
		data = {
			"cabang_code": row[0],
			"description": row[1],
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
	INSERT INTO `sl_01_m_cabang`
		(description, active, create_by, create_date)
	VALUES
		(%s, %s, %s, %s)
	"""

	cur.execute(sqlInsert, ( req['description'], req['active'], vToken['id_user'], datetime.date.today()))
	id = con.insert_id()
	inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionCreate(), id, '', master_table_name(), vToken['id_user'])
	con.commit()

	sql = "SELECT cabang_code, description, active FROM `sl_01_m_cabang` WHERE cabang_code = %s"
	cur.execute(sql, (id))
	row = cur.fetchone()
	data = {
		"cabang_code": row[0],
		"description": row[1],
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
    UPDATE `sl_01_m_cabang`
    SET
        `description` = %s,
        `active` = %s,
        `update_by` = %s,
        `update_date` = %s
    WHERE
        cabang_code = %s
    """
    
    cur.execute(sqlUpdate, ( req['description'], req['active'], vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionEdit(), id, '', master_table_name(), vToken['id_user'])
    con.commit()
    
    sql = "SELECT cabang_code, description, active FROM `sl_01_m_cabang` WHERE cabang_code = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "cabang_code": row[0],
        "description": row[1],
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
    UPDATE `sl_01_m_cabang` 
    SET 
        delete_by = %s, 
        delete_date = %s, 
        delete_mark = 1 
    WHERE 
        cabang_code = %s
    """
    
    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionDelete(), id, '', master_table_name(), vToken['id_user'])
    con.commit()
    
    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)