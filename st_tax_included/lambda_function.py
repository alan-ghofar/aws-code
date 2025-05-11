import json
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


# id menu cek table sc_02_menu
def get_id_menu():
    return '65'


def master_table_name():
    return 'st_01_tax_included'


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

    sql = "SELECT id, nama_tax_inc, description FROM `st_01_tax_included` WHERE delete_mark = 0 AND active = 1"

    if params is not None:
        if params.get('id'):
            sql += " AND id = '" + params.get('id') + "'"

    cur.execute(sql)
    result = cur.fetchall()

    allData = []
    for row in result:
        sqlChild = """
            SELECT
                tci.code_tax_category,
                tc.description 
            FROM
                `st_02_tax_category_included` tci
                LEFT JOIN `st_01_tax_category` tc ON ( tc.code_tax_category = tci.code_tax_category AND tc.delete_mark = 0 AND tc.active = 1 ) 
            WHERE
                tci.delete_mark = 0 
                AND tci.active = 1
                AND tci.id_tax_included = %s
        """
        cur.execute(sqlChild, (row[0]))
        resChild = cur.fetchall()
        allChild = []
        for rowChild in resChild:
            child = {
                "code_tax_category": rowChild[0],
                "description": rowChild[1]
            }
            allChild.append(child)

        data = {
            "id": row[0],
            "nama_tax_inc": row[1],
            "description": row[2],
            "child": allChild
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
        INSERT INTO `st_01_tax_included`
            (nama_tax_inc, description, active, create_by, create_date, delete_mark)
        VALUES
            (%s, %s, %s, %s, %s, 0)
        """

        cur.execute(sqlInsert, (req['nama_tax_inc'], req['description'], req['active'], vToken['id_user'], datetime.date.today()))
        id = con.insert_id()

        for row in req['items']:
            id_tax_included = id
            code_tax_category = row['code_tax_category']

            sqlInsert = """
            INSERT INTO `st_02_tax_category_included` 
                ( id_tax_included, code_tax_category, active, create_by, create_date, delete_mark ) 
            VALUES 
                (%s, %s, %s, %s, %s, 0)
            """
            cur.execute(sqlInsert, (id_tax_included, code_tax_category, 1, vToken['id_user'], datetime.date.today()))

        inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionCreate(), id, '', master_table_name(), vToken['id_user'])
    except Exception as e: 
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    sql = "SELECT id, nama_tax_inc, description FROM `st_01_tax_included` WHERE id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()

    sqlChild = """
        SELECT
            tci.code_tax_category,
            tc.description 
        FROM
            `st_02_tax_category_included` tci
            LEFT JOIN `st_01_tax_category` tc ON ( tc.code_tax_category = tci.code_tax_category AND tc.delete_mark = 0 AND tc.active = 1 ) 
        WHERE
            tci.delete_mark = 0 
            AND tci.active = 1
            AND tci.id_tax_included = %s
    """
    cur.execute(sqlChild, (id))
    resChild = cur.fetchall()
    allChild = []
    for rowChild in resChild:
        child = {
            "code_tax_category": rowChild[0],
            "description": rowChild[1]
        }
        allChild.append(child)
    data = {
        "id": row[0],
        "nama_tax_inc": row[1],
        "description": row[2],
        "child": allChild
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
        # Delete data st_02_tax_category_included with id_tax_included X
        sqlDelete = "DELETE FROM `st_02_tax_category_included` WHERE id_tax_included = " + id
        cur.execute(sqlDelete)
        
        sqlUpdate = """
        UPDATE `st_01_tax_included`
        SET
        	nama_tax_inc = %s,
        	description = %s,
        	active = %s,
        	update_by = %s,
        	update_date = %s
        WHERE
        	id = %s
        """
        cur.execute(sqlUpdate, (req['nama_tax_inc'], req['description'], req['active'], vToken['id_user'], datetime.date.today(), id))
        
        # INSERT KE DETAIL
        for row in req['items']:
            id_tax_included = id
            code_tax_category = row['code_tax_category']
        
            sqlInsert = """
            INSERT INTO `st_02_tax_category_included` 
                ( id_tax_included, code_tax_category, active, create_by, create_date, delete_mark ) 
            VALUES 
                (%s, %s, %s, %s, %s, 0)
            """
            cur.execute(sqlInsert, (id_tax_included, code_tax_category, 1, vToken['id_user'], datetime.date.today()))
        
        inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionEdit(), id, '', master_table_name(), vToken['id_user'])
    except Exception as e: 
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()


    sql = "SELECT id, nama_tax_inc, description FROM `st_01_tax_included` WHERE id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    
    sqlChild = """
        SELECT
            tci.code_tax_category,
            tc.description 
        FROM
            `st_02_tax_category_included` tci
            LEFT JOIN `st_01_tax_category` tc ON ( tc.code_tax_category = tci.code_tax_category AND tc.delete_mark = 0 AND tc.active = 1 ) 
        WHERE
            tci.delete_mark = 0 
            AND tci.active = 1
            AND tci.id_tax_included = %s
    """
    cur.execute(sqlChild, (id))
    resChild = cur.fetchall()
    allChild = []
    for rowChild in resChild:
        child = {
            "code_tax_category": rowChild[0],
            "description": rowChild[1]
        }
        allChild.append(child)
    data = {
        "id": row[0],
        "nama_tax_inc": row[1],
        "description": row[2],
        "child": allChild
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
	UPDATE `st_01_tax_included` 
	SET 
		delete_by = %s, 
		delete_date = %s, 
		delete_mark = 1 
	WHERE 
		id = %s
	"""
    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), id))
    
    sqlDeleteChild = """
	UPDATE `st_02_tax_category_included` 
	SET 
		delete_by = %s, 
		delete_date = %s, 
		delete_mark = 1 
	WHERE 
		id_tax_included = %s
	"""
    cur.execute(sqlDeleteChild, (vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionDelete(), id, '', master_table_name(), vToken['id_user'])
    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
