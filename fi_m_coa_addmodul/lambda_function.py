import json
import pymysql.cursors
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

tableName = 'fi_04_m_coa_addmodul'

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
            addmod.id,
        	addmod.account_code,
        	coa.account_name,
        	addmod.type_doc,
        	doc.description AS doc_name,
        	addmod.active 
        FROM
        	fi_04_m_coa_addmodul AS addmod
        	LEFT JOIN fi_03_m_chart_of_account AS coa ON coa.account_code = addmod.account_code
        	LEFT JOIN st_01_document AS doc ON doc.type_doc = addmod.type_doc 
        WHERE
        	addmod.delete_mark = 0 
        	AND addmod.id LIKE %s
        	AND addmod.type_doc LIKE %s 
        	AND addmod.account_code LIKE %s 
        	AND addmod.active LIKE %s
        	
        """
    filterId = '%'
    filterTypeDoc = '%'
    filterAccountCode = '%'
    filterActive = '1'
    
    if params is not None:
        if params.get('id'):
            filterId = params.get('id')
        if params.get('type_doc'):
            filterTypeDoc = params.get('type_doc')
        if params.get('account_code'):
            filterAccountCode = params.get('account_code')
        if params.get('show_inactive')=="true":
            filterActive = '%'
    
    cur.execute(sql, (filterId, filterTypeDoc, filterAccountCode, filterActive))
    myresult = cur.fetchall()
    allData = []
    for row in myresult:
        data = {
            "id" : row['id'],
            "account_code" : row['account_code'],
            "account_name" : row['account_name'],
            "type_doc" : row['type_doc'],
            "doc_name" : row['doc_name'],
            "active" : row['active']
        }
        allData.append(data)
    
    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    # return inc_def.send_response_data(event, 400)
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    
    sql = """
        INSERT INTO `fi_04_m_coa_addmodul` 
            (
                account_code,
                type_doc,
                active, 
                create_by, 
                create_date
            ) 
        VALUES 
            (%s, %s, %s, %s, %s)
        """
    cur.execute(sql, (
            req['account_code'], 
            req['type_doc'], 
            req['active'], 
            vToken['id_user'], 
            datetime.date.today()
        ))
    id=con.insert_id()
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])

    
    con.commit()
    
    sql = """
	SELECT
            addmod.id,
        	addmod.account_code,
        	coa.account_name,
        	addmod.type_doc,
        	doc.description AS doc_name,
        	addmod.active 
        FROM
        	fi_04_m_coa_addmodul AS addmod
        	LEFT JOIN fi_03_m_chart_of_account AS coa ON coa.account_code = addmod.account_code
        	LEFT JOIN st_01_document AS doc ON doc.type_doc = addmod.type_doc 
        WHERE
        	addmod.delete_mark = 0 
        	AND addmod.id LIKE %s
    """
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
             "id" : row['id'],
            "account_code" : row['account_code'],
            "account_name" : row['account_name'],
            "type_doc" : row['type_doc'],
            "doc_name" : row['doc_name'],
            "active" : row['active']
    }
    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    # return inc_def.send_response_data(event, 400)
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
   
    sqlUpdateStockMaster = """
    UPDATE `fi_04_m_coa_addmodul` 
    SET 
        `account_code` = %s,
    	`active` = %s,
        `update_by` = %s, 
        `update_date` = %s
    WHERE 
       `id` = %s
    """
    cur.execute(sqlUpdateStockMaster, (req['account_code'], req['active'], 
        vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    
    con.commit()

    sql = """
		SELECT
            addmod.id,
        	addmod.account_code,
        	coa.account_name,
        	addmod.type_doc,
        	doc.description AS doc_name,
        	addmod.active 
        FROM
        	fi_04_m_coa_addmodul AS addmod
        	LEFT JOIN fi_03_m_chart_of_account AS coa ON coa.account_code = addmod.account_code
        	LEFT JOIN st_01_document AS doc ON doc.type_doc = addmod.type_doc 
        WHERE
        	addmod.delete_mark = 0 
        	AND addmod.id LIKE %s
    """
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
           "id" : row['id'],
            "account_code" : row['account_code'],
            "account_name" : row['account_name'],
            "type_doc" : row['type_doc'],
            "doc_name" : row['doc_name'],
            "active" : row['active']
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    
    sqlDelete = "UPDATE `fi_04_m_coa_addmodul` SET delete_by = %s, delete_date = %s, delete_mark = %s WHERE id = %s"
    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])

    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
