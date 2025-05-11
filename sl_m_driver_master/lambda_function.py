import json
import datetime
import pymysql.cursors
import valid_token as v_token
import valid_menu as v_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db
import dateutil.tz


# id menu cek table sc_02_menu
tableName = 'sl_02_m_driver_master'


def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 404)

    global idMenu
    global tz_JKT
    
    tz_JKT = dateutil.tz.gettz('Asia/Jakarta')

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

    allData = []
    try:
        sql = """
        SELECT
            drv.`driver_id`,
            drv.`ktp`,
            drv.`fullname`,
            drv.`birth_place`,
            drv.`birth_date`,
            drv.`gender`,
            drv.`status_marriage`,
            drv.`jml_tanggungan`,
            drv.`jenis_sim`,
            drv.`no_sim`,
            drv.`ethnic`,
            drv.`nationality`,
            drv.`address`,
            drv.`kode_lokasi`,
            drv.`start_date`,
            drv.`end_date`,
            drv.`no_hp_darurat`,
            drv.`no_hp`,
            drv.`email`,
            drv.`no_bpjstku`,
            drv.`status`,
            drv.`piutang_account`,
            drv.`available`,
            lok.nama_lokasi
        FROM
            sl_02_m_driver_master drv
            LEFT JOIN st_01_lokasi lok ON ( lok.kode_lokasi = drv.kode_lokasi ) 
        WHERE
            drv.delete_mark = 0 
            AND drv.active = 1
        """
        if params is not None:
            if params.get('id'):
                sql += " AND drv.`driver_id` = '" + params.get('id') + "'"

        cur.execute(sql)
        myresult = cur.fetchall()

        for row in myresult:
            data = {
                "driver_id": row['driver_id'],
                "ktp": row['ktp'],
                "fullname": row['fullname'],
                "birth_place": row['birth_place'],
                "birth_date": row['birth_date'],
                "gender": "Male" if row['gender'] == 1 else "Female",
                "status_marriage": getStatusMarriage(row['status_marriage']),
                "jml_tanggungan": row['jml_tanggungan'],
                "jenis_sim": row['jenis_sim'],
                "no_sim": row['no_sim'],
                "ethnic": row['ethnic'],
                "nationality": row['nationality'],
                "address": row['address'],
                "kode_lokasi": row['kode_lokasi'],
                "nama_lokasi": row['nama_lokasi'],
                "start_date": row['start_date'],
                "end_date": row['end_date'],
                "no_hp_darurat": row['no_hp_darurat'],
                "no_hp": row['no_hp'],
                "email": row['email'],
                "no_bpjstku": row['no_bpjstku'],
                "status": "Active" if row['status'] == 1 else "Resign",
                "piutang_account": row['piutang_account'],
                "available": "Available" if row['available'] == 0 else "Used",
                
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

    try:
        id = getMaxId(cur) + 1
        sqlInsert = """
        INSERT INTO `sl_02_m_driver_master` 
            ( 
                `driver_id`, `ktp`, `fullname`, `birth_place`, `birth_date`, `gender`, `status_marriage`, 
                `jml_tanggungan`, `jenis_sim`, `no_sim`, `ethnic`, `nationality`, `address`, 
                `kode_lokasi`, `start_date`, `end_date`, `no_hp_darurat`, `no_hp`, `email`, 
                `no_bpjstku`, `status`, `piutang_account`, `available`,
                active, create_by, create_date, delete_mark 
            ) 
        VALUES 
            (
                %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, 
                %s, %s, %s, 0
            )
        """
        cur.execute(sqlInsert, (id, req['ktp'], req['fullname'], req['birth_place'], req['birth_date'], req['gender'], req['status_marriage'],
                                req['jml_tanggungan'], req['jenis_sim'], req['no_sim'], req['ethnic'], req['nationality'], req['address'], 
                                req['kode_lokasi'], req['start_date'], req['end_date'], req['no_hp_darurat'], req['no_hp'], req['email'],
                                req['no_bpjstku'], req['status'], req['piutang_account'], req['available'], 
                                req['active'], vToken['id_user'], datetime.datetime.now(tz_JKT)))

        
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
        UPDATE `sl_02_m_driver_master` 
        SET
            `ktp` = %s, 
            `fullname` = %s, 
            `birth_place` = %s, 
            `birth_date` = %s, 
            `gender` = %s, 
            `status_marriage` = %s, 
            `jml_tanggungan` = %s, 
            `jenis_sim` = %s, 
            `no_sim` = %s, 
            `ethnic` = %s, 
            `nationality` = %s, 
            `address` = %s, 
            `kode_lokasi` = %s, 
            `start_date` = %s, 
            `end_date` = %s, 
            `no_hp_darurat` = %s, 
            `no_hp` = %s, 
            `email` = %s, 
            `no_bpjstku` = %s, 
            `status` = %s, 
            `piutang_account` = %s, 
            `active` = %s, 
            update_by = %s, 
            update_date = %s
        WHERE 
            `driver_id` = %s
        """
        cur.execute(sqlUpdate, (
            req['ktp'], req['fullname'], req['birth_place'], req['birth_date'], req['gender'], req['status_marriage'],
            req['jml_tanggungan'], req['jenis_sim'], req['no_sim'], req['ethnic'], req['nationality'], req['address'], 
            req['kode_lokasi'], req['start_date'], req['end_date'], req['no_hp_darurat'], req['no_hp'], req['email'],
            req['no_bpjstku'], req['status'], req['piutang_account'], 
            req['active'], vToken['id_user'], datetime.datetime.now(tz_JKT), id
        ))
        inc_db.addAuditMaster(
            con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
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
    UPDATE `sl_02_m_driver_master` 
    SET 
        delete_by = %s, 
        delete_date = %s, 
        delete_mark = 1 
    WHERE 
        driver_id = %s
    """

    cur.execute(sqlDelete, (vToken['id_user'], datetime.datetime.now(tz_JKT), id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(),
                          id, '', tableName, vToken['id_user'])
    con.commit()

    cur.close()
    return inc_def.send_response_data("Deleted!", 200)


def getStatusMarriage(key_status):
    statusName = {
        1: "Single",
        2: "Married",
        3: "Widowed",
        4: "divorced",
    }

    for key, value in statusName.items():
        if key_status == key:
            return value

def getMaxId(cur):
    sql = """SELECT IFNULL(MAX(driver_id),0) driver FROM sl_02_m_driver_master"""

    cur.execute(sql)
    row = cur.fetchone()
    return row[0]