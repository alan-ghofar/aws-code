import json
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


tableName = 'sl_03_m_grouping_price_list_wilayah'


def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 403)
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
    con.begin()
    cur = con.cursor()
    params = event['queryStringParameters']

    try:
        sql = """
        SELECT
            gpwil.id,
            gpwil.gpl_id,
            gp.description,
            gpwil.wilayah_code,
            wil.description wilayah_name
        FROM
            sl_03_m_grouping_price_list_wilayah gpwil
            LEFT JOIN sl_02_m_grouping_price_list gp ON ( gp.id = gpwil.gpl_id )
            LEFT JOIN sl_02_m_wilayah wil ON ( wil.wilayah_code = gpwil.wilayah_code ) 
        WHERE
            gpwil.delete_mark = 0 
            AND gpwil.active = 1
        """
        if params is not None:
            if params.get('id'):
                sql += " AND gpwil.id = " + params.get('id') 
    
        cur.execute(sql)
        myresult = cur.fetchall()
    
        allData = []
        for row in myresult:
            data = {
                "id": row[0],
                "gpl_id": row[1],
                "description": row[2],
                "wilayah_code": row[3],
                "wilayah_name": row[4],
            }
            allData.append(data)
    
        cur.close()
        return inc_def.send_response_data(allData, 200)
    except Exception as e:
        con.close()
        return inc_def.send_response_data(str(e), 500)


# =============================== FUNCTION POST
def functionPost(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    field = {
        "gpl_id": req['gpl_id'],
        "wilayah_code": req['wilayah_code']
    }
    
    isDouble = inc_db.checkDoubleData(cur, field, tableName)
    
    if isDouble == "true" :
        return inc_def.send_response_data("Data is exist", 500)

    try :
        sqlInsert = """
        INSERT INTO `sl_03_m_grouping_price_list_wilayah` 
            ( gpl_id, wilayah_code, active, create_by, create_date, delete_mark ) 
        VALUES 
            (%s, %s, %s, %s, %s, 0)
        """
        cur.execute(sqlInsert, (req['gpl_id'], req['wilayah_code'], req['active'], 
            vToken['id_user'], datetime.date.today()))
        # Ambil id tersimpan
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
        gpwil.id,
        gpwil.gpl_id,
        gp.description,
        gpwil.wilayah_code 
    FROM
        sl_03_m_grouping_price_list_wilayah gpwil
        LEFT JOIN sl_02_m_grouping_price_list gp ON ( gp.id = gpwil.gpl_id ) 
    WHERE
        gpwil.delete_mark = 0 
        AND gpwil.active = 1
        AND gpwil.id = %s
    """
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "gpl_id": row[1],
        "description": row[2],
        "wilayah_code": row[3]
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
        "wilayah_code": req['wilayah_code']
    }
    
    isDouble = inc_db.checkDoubleData(cur, field, tableName)
    
    if isDouble == "true" :
        return inc_def.send_response_data("Data is exist", 500)
    
    try :
        sqlUpdate = """
        UPDATE `sl_03_m_grouping_price_list_wilayah` 
        SET 
            `gpl_id` = %s,
            `wilayah_code` = %s, 
            `update_by` = %s, 
            `update_date` = %s, 
            `active` = %s 
        WHERE 
            `id` = %s
        """
        cur.execute(sqlUpdate, (req['gpl_id'], req['wilayah_code'], 
            vToken['id_user'], datetime.date.today(), req['active'], id))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    
    else:
        con.commit()

    sql = """
    SELECT
        gpwil.id,
        gpwil.gpl_id,
        gp.description,
        gpwil.wilayah_code 
    FROM
        sl_03_m_grouping_price_list_wilayah gpwil
        LEFT JOIN sl_02_m_grouping_price_list gp ON ( gp.id = gpwil.gpl_id ) 
    WHERE
        gpwil.delete_mark = 0 
        AND gpwil.active = 1
        AND gpwil.id = %s
    """
    cur.execute(sql, (params['id']))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "gpl_id": row[1],
        "description": row[2],
        "wilayah_code": row[3]
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
    UPDATE `sl_03_m_grouping_price_list_wilayah` 
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