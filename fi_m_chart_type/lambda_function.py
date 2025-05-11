import json
import datetime
import pymysql.cursors
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

tableName = 'fi_02_m_chart_type'

def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 200)
    
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
    dataMenu = inc_db.getDataMenu(con, idMenu)
    pathMenunya = dataMenu['path'] #inc_db.getPathMenu(con, idMenu)
    
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

    allData = []
    try:
        sql = """
        SELECT
        	mct.type_id,
        	mct.type_name,
        	mct.class_id,
        	mcc.class_name,
        	mct.parent_type,
        	mct1.type_name AS parent_name
        FROM
        	fi_02_m_chart_type AS mct
        	LEFT JOIN fi_01_m_chart_class AS mcc ON (mcc.class_id=mct.class_id)
        	LEFT JOIN fi_02_m_chart_type AS mct1 ON (mct1.type_id=mct.parent_type)
        """
        if params is not None:
            if params.get('id'):
                sql += " AND mct.`type_id` = '" + params.get('id') + "'"

        cur.execute(sql)
        myresult = cur.fetchall()

        for row in myresult:
            data = {
                "type_id": row['type_id'],
                "type_name": row['type_name'],
                "class_id": row['class_id'],
                "parent_type": row['parent_type'],
                "class_name": row['class_name'],
                "parent_name": row['parent_name'],
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
        "type_id": req['type_id'],
    }
    
    isDouble = inc_db.checkDoubleData(cur, field, tableName)
    
    if isDouble == "true" :
        return inc_def.send_response_data("Data is exist", 500)

    try:
        sqlInsert = """
        INSERT INTO `fi_02_m_chart_type` 
            ( 
                `type_id`, `type_name`, `class_id`, `parent_type`,
                active, create_by, create_date, delete_mark 
            ) 
        VALUES 
            (
                %s, %s, %s, %s, 
                %s, %s, %s, 0
            )
        """
        cur.execute(sqlInsert, (
            req['type_id'], req['type_name'], req['class_id'], req['parent_type'],
            req['active'], vToken['id_user'], datetime.date.today())
        )
        id = req['type_id']

        
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
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']

    try:
        sqlUpdate = """
        UPDATE `fi_02_m_chart_type` 
        SET
            type_id = %s, 
            type_name = %s, 
            class_id = %s, 
            parent_type = %s, 
            `active` = %s, 
            update_by = %s, 
            update_date = %s
        WHERE 
            `type_id` = %s
        """
        cur.execute(sqlUpdate, (
            req['type_id'], req['type_name'], req['class_id'], req['parent_type'], 
            req['active'], vToken['id_user'], datetime.date.today(), id
        ))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
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
    UPDATE `fi_02_m_chart_type` 
    SET 
        delete_by = %s, 
        delete_date = %s, 
        delete_mark = 1 
    WHERE 
        type_id = %s
    """

    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    con.commit()

    cur.close()
    return inc_def.send_response_data("Deleted!", 200)
