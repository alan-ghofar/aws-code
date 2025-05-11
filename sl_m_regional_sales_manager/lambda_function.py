import json
import datetime
import pymysql.cursors
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

#id menu cek table sc_02_menu
tableName = 'sl_01_m_regional_sales_manager'

def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 500)
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

    allData = []
    try:
        sql = """
        SELECT
            rsm.rsm_id,
            rsm.rsm_code,
            rsm.rsm_name,
            rsm.rsm_phone,
            rsm.rsm_email,
            rsm.place_of_birth,
            rsm.date_of_birth,
            rsm.work_start_date,
            rsm.`status`,
            rsm.user_id,
            u.nama_user,
            u.nama_lengkap
        FROM
            `sl_01_m_regional_sales_manager` rsm
            LEFT JOIN `sc_01_user` u ON ( u.id = rsm.user_id ) 
        WHERE
            rsm.`delete_mark` = 0 
            AND rsm.`active` = 1
        """
        if params is not None:
            if params.get('id'):
                sql += " AND rsm.rsm_id = '" + params.get('id') + "'"

        cur.execute(sql)
        myresult = cur.fetchall()

        for row in myresult:
            data = {
                "rsm_id": row['rsm_id'],
                "rsm_code": row['rsm_code'],
                "rsm_name": row['rsm_name'],
                "rsm_phone": row['rsm_phone'],
                "rsm_email": row['rsm_email'],
                "place_of_birth": row['place_of_birth'],
                "date_of_birth": row['date_of_birth'],
                "work_start_date": row['work_start_date'],
                "status": row['status'],
                "status_name": getStatusName(row['status']),
                "user_id": row['user_id'],
                "nama_user": row['nama_user'],
                "nama_lengkap": row['nama_lengkap'],
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
        "rsm_code": req['rsm_code'],
    }
    
    isDouble = inc_db.checkDoubleData(cur, field, tableName)
    
    if isDouble == "true" :
        return inc_def.send_response_data("Data is exist", 500)

    try :
        sqlInsert = """
        INSERT INTO `sl_01_m_regional_sales_manager` 
            ( 
                rsm_code, rsm_name, rsm_phone, rsm_email, place_of_birth, 
                date_of_birth, work_start_date, `status`, user_id, 
                active, create_by, create_date, delete_mark 
            ) 
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
        """
        cur.execute(sqlInsert, (req['rsm_code'], req['rsm_name'], req['rsm_phone'], req['rsm_email'], req['place_of_birth'], 
        req['date_of_birth'], req['work_start_date'], req['status'], req['user_id'], 
        req['active'], vToken['id_user'], datetime.date.today()))
        
        id = con.insert_id()
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
            rsm.rsm_id,
            rsm.rsm_code,
            rsm.rsm_name,
            rsm.rsm_phone,
            rsm.rsm_email,
            rsm.place_of_birth,
            rsm.date_of_birth,
            rsm.work_start_date,
            rsm.`status`,
            rsm.user_id,
            u.nama_lengkap
        FROM
            `sl_01_m_regional_sales_manager` rsm
            LEFT JOIN `sc_01_user` u ON ( u.id = rsm.user_id ) 
        WHERE
            rsm.`delete_mark` = 0 
            AND rsm.`active` = 1
            AND rsm.rsm_id = %s
        """
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "rsm_id": row['rsm_id'],
        "rsm_code": row['rsm_code'],
        "rsm_name": row['rsm_name'],
        "rsm_phone": row['rsm_phone'],
        "rsm_email": row['rsm_email'],
        "place_of_birth": row['place_of_birth'],
        "date_of_birth": row['date_of_birth'],
        "work_start_date": row['work_start_date'],
        "status": row['status'],
        "status_name": getStatusName(row['status']),
        "user_id": row['user_id'],
        "nama_lengkap": row['nama_lengkap'],
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']

    try :
        sqlUpdate = """
        UPDATE `sl_01_m_regional_sales_manager` 
        SET
            rsm_code = %s, 
            rsm_name = %s, 
            rsm_phone = %s, 
            rsm_email = %s, 
            place_of_birth = %s, 
            date_of_birth = %s, 
            work_start_date = %s, 
            `status` = %s, 
            user_id = %s, 
            active = %s, 
            update_by = %s, 
            update_date = %s
        WHERE 
            `rsm_id` = %s
        """
        cur.execute(sqlUpdate, (
            req['rsm_code'], req['rsm_name'], req['rsm_phone'], req['rsm_email'], req['place_of_birth'], 
            req['date_of_birth'], req['work_start_date'], req['status'], req['user_id'], 
            req['active'], vToken['id_user'], datetime.date.today(), id
        ))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    sql = """
        SELECT
            rsm.rsm_id,
            rsm.rsm_code,
            rsm.rsm_name,
            rsm.rsm_phone,
            rsm.rsm_email,
            rsm.place_of_birth,
            rsm.date_of_birth,
            rsm.work_start_date,
            rsm.`status`,
            rsm.user_id,
            u.nama_lengkap
        FROM
            `sl_01_m_regional_sales_manager` rsm
            LEFT JOIN `sc_01_user` u ON ( u.id = rsm.user_id ) 
        WHERE
            rsm.`delete_mark` = 0 
            AND rsm.`active` = 1
            AND rsm.rsm_id = %s
        """
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "rsm_id": row['rsm_id'],
        "rsm_code": row['rsm_code'],
        "rsm_name": row['rsm_name'],
        "rsm_phone": row['rsm_phone'],
        "rsm_email": row['rsm_email'],
        "place_of_birth": row['place_of_birth'],
        "date_of_birth": row['date_of_birth'],
        "work_start_date": row['work_start_date'],
        "status": row['status'],
        "status_name": getStatusName(row['status']),
        "user_id": row['user_id'],
        "nama_lengkap": row['nama_lengkap'],
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
    UPDATE `sl_01_m_regional_sales_manager` 
    SET 
        delete_by = %s, 
        delete_date = %s, 
        delete_mark = 1 
    WHERE 
        rsm_id = %s
    """
    
    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    con.commit()
    
    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


def getStatusName(key_status):
    statusName = {
        3: "Resign", 
        2: "Training", 
        1: "Active"
    }
    
    for key, value in statusName.items():
        if key_status == key:
            return value