import json
import datetime
import pymysql.cursors
import valid_token as v_token
import valid_menu as v_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


tableName = 'st_01_tax_groups'


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
    	tg.tax_group_id,
    	tg.`name` tax_group_name,
    	tg.tax_shipping,
    	tgi.tax_type_id,
    	tt.`name` tax_type_name,
    	tt.rate 
    FROM
    	`st_01_tax_groups` tg
    	LEFT JOIN `st_03_tax_group_items` tgi ON ( tgi.tax_group_id = tg.tax_group_id AND tgi.delete_mark = 0 AND tgi.active = 1 )
    	LEFT JOIN `st_02_tax_types` tt ON ( tt.tax_type_id = tgi.tax_type_id AND tt.delete_mark = 0 AND tt.active = 1 ) 
    WHERE
    	tg.delete_mark = 0 
    	AND tg.active = 1 
    """
	
    if params is not None:
        if params.get('id'):
            sql += " AND tg.tax_group_id LIKE '%" + params.get('id') + "%'"
            
    sql += """
    ORDER BY
	    tg.tax_group_id
    """

    cur.execute(sql)
    result = cur.fetchall()

    returnData = []
    lineItemData = []
    headerData = []
    tempTGI=0
    for row in result:
        tgi = row['tax_group_id']
        if tempTGI != 0 and tgi != tempTGI:
            headerData['line_items'] = lineItemData
            lineItemData = []
            returnData.append(headerData)
            
        headerData = {
            "tax_group_id": row["tax_group_id"],
            "tax_group_name": row["tax_group_name"],
            "tax_shipping": row["tax_shipping"]
        }
        
        linenya = {
            "tax_type_id": row['tax_type_id'],
            "tax_type_name": row['tax_type_name'],
            "rate": row['rate'],
        }
        
        lineItemData.append(linenya)
        tempTGI = tgi
    
    if(len(lineItemData) > 0):
        headerData['line_items'] = lineItemData
    lineItemData = []
    returnData.append(headerData)
    
    cur.close()
    return inc_def.send_response_data(returnData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    try:
        sqlInsert = """
    	INSERT INTO `st_01_tax_groups`
    		(name, tax_shipping, active, create_by, create_date, delete_mark)
    	VALUES
            (%s, %s, %s, %s, %s, 0)
        """
        cur.execute(sqlInsert, ( req['name'], req['tax_shipping'], req['active'], vToken['id_user'], datetime.date.today()))
        id = con.insert_id()
        
        for row in req['tax_type']:
            tax_group_id = id
            tax_type_id = row['id']
            tax_type_rate = row['rate']
    
            sqlInsert = """
            INSERT INTO `st_03_tax_group_items` 
                ( tax_group_id, tax_type_id, rate, active, create_by, create_date, delete_mark ) 
            VALUES 
                (%s, %s, %s, %s, %s, %s, 0)
            """
            cur.execute(sqlInsert, (tax_group_id, tax_type_id, tax_type_rate, req['active'], vToken['id_user'], datetime.date.today()))
            
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
        UPDATE `st_01_tax_groups`
        SET
            name = %s,
            tax_shipping = %s,
            active = %s,
            update_by = %s,
            update_date = %s
        WHERE
            tax_group_id = %s
    	"""
        cur.execute(sqlUpdate, (req['name'], req['tax_shipping'], req['active'], vToken['id_user'], datetime.date.today(), id))
        
        sqlDelete = "DELETE FROM `st_03_tax_group_items` WHERE tax_group_id = " + id
        cur.execute(sqlDelete)
        
        for row in req['tax_type']:
            tax_group_id = id
            tax_type_id = row['id']
            tax_type_rate = row['rate']
            
            sqlInsert = """
            INSERT INTO `st_03_tax_group_items` 
                ( tax_group_id, tax_type_id, rate, active, create_by, create_date, delete_mark ) 
            VALUES 
                (%s, %s, %s, %s, %s, %s, 0)
            """
            cur.execute(sqlInsert, (tax_group_id, tax_type_id, tax_type_rate, req['active'], vToken['id_user'], datetime.date.today()))
            
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
    UPDATE `st_01_tax_groups` 
    SET 
        delete_by = %s, 
        delete_date = %s, 
        delete_mark = 1 
    WHERE 
        tax_group_id = %s
    """
    
    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    con.commit()
    
    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)