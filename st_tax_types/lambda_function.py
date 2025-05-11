import json
import datetime
import pymysql.cursors
import valid_token as v_token
import valid_menu as v_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


tableName = 'st_02_tax_types'


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
    cur = con.cursor(pymysql.cursors.DictCursor)
    params = event['queryStringParameters']

    sql = """
    SELECT
    	tt.tax_type_id,
    	tt.rate,
    	tt.sales_gl_code,
    	tt.purchasing_gl_code,
    	tt.`name`,
    	tt.code_tax_category,
    	tc.description desc_tax_category
    FROM
    	`st_02_tax_types` tt
    	LEFT JOIN st_01_tax_category tc ON ( tc.code_tax_category = tt.code_tax_category ) 
    WHERE
    	tt.delete_mark = 0 
    	AND tt.active = 1
    """

    if params is not None:
        if params.get('id'):
            sql += " AND tt.tax_type_id = " + params.get('id')

    cur.execute(sql)
    result = cur.fetchall()

    allData = []
    for row in result:
        data = {
            "tax_type_id": row["tax_type_id"],
            "rate": row["tax_type_id"],
            "sales_gl_code": row["sales_gl_code"],
            "purchasing_gl_code": row["purchasing_gl_code"],
            "name": row["name"],
            "code_tax_category": row["code_tax_category"],
            "desc_tax_category": row["desc_tax_category"],
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
    
    try:
        sqlInsert = """
    	INSERT INTO `st_02_tax_types`
    		(rate, sales_gl_code, purchasing_gl_code, name, code_tax_category, active, create_by, create_date, delete_mark)
    	VALUES
    		(%s, %s, %s, %s, %s, %s, %s, %s, 0)
    	"""
    
        cur.execute(sqlInsert, (req['rate'], req['sales_gl_code'], req['purchasing_gl_code'], req['name'], req['code_tax_category'], req['active'], vToken['id_user'], datetime.date.today()))
        id = con.insert_id()
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e) , 500)
    else:
        con.commit()

    cur.close()
    return inc_def.send_response_data("Inserted!", 200)


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
    	UPDATE `st_02_tax_types`
    	SET
    		rate = %s,
    		sales_gl_code = %s,
    		purchasing_gl_code = %s,
    		name = %s,
    		code_tax_category = %s,
    		active = %s,
    		update_by = %s,
    		update_date = %s
    	WHERE
    		tax_type_id = %s
    	"""
    
        cur.execute(sqlUpdate, (req['rate'], req['sales_gl_code'], req['purchasing_gl_code'], req['name'], req['code_tax_category'], req['active'], vToken['id_user'], datetime.date.today(), id))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e) , 500)
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
    sqlDelete = """
	UPDATE `st_02_tax_types` 
	SET 
		delete_by = %s, 
		delete_date = %s, 
		delete_mark = 1 
	WHERE 
		tax_type_id = %s
	"""
    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    con.commit()

    cur.close()
    return inc_def.send_response_data("Deleted!", 200)
