import json
import datetime
import pymysql.cursors
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


#id menu cek table sc_02_menu
tableName = 'sl_04_m_kelurahan'


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
    	kel.kelurahan_code,
    	kel.description kelurahan_name,
    	kec.kecamatan_code,
    	kec.description kecamatan_name,
    	kab.kabupaten_code,
    	kab.description kabupaten_name,
    	prov.provinsi_code,
    	prov.description provinsi_name 
    FROM
    	sl_04_m_kelurahan kel
    	LEFT JOIN sl_03_m_kecamatan kec ON ( kec.kecamatan_code = kel.kecamatan_code )
    	LEFT JOIN sl_02_m_kabupaten kab ON ( kab.kabupaten_code = kec.kabupaten_code )
    	LEFT JOIN sl_01_m_provinsi prov ON ( prov.provinsi_code = kab.provinsi_code ) 
    WHERE
        kel.delete_mark = 0 
        AND kel.active = 1
    """
    if params is not None:
        if params.get('id'):
            sql += " AND kel.kelurahan_code = " + params.get('id') 
        if params.get('kecamatan_code'):
            sql += " AND kel.kecamatan_code = " + params.get('kecamatan_code') 

    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        data = {
            "kelurahan_code": row["kelurahan_code"],
            "kelurahan_name": row["kelurahan_name"],
            "kecamatan_code": row["kecamatan_code"],
            "kecamatan_name": row["kecamatan_name"],
            "kabupaten_code": row["kabupaten_code"],
            "kabupaten_name": row["kabupaten_name"],
            "provinsi_code": row["provinsi_code"],
            "provinsi_name": row["provinsi_name"],
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
    
    field = {
        "kelurahan_code": req['kelurahan_code'],
    }

    isDouble = inc_db.checkDoubleData(cur, field, tableName)
    
    if isDouble == "true" :
        return inc_def.send_response_data("Data is exist", 500)

    try:
        sqlInsert = """
        INSERT INTO `sl_04_m_kelurahan` 
            ( kelurahan_code, description, kecamatan_code, active, create_by, create_date, delete_mark ) 
        VALUES 
            (%s, %s, %s, %s, %s, %s, 0)
        """
        cur.execute(sqlInsert, (req['kelurahan_code'], req['description'], req['kecamatan_code'], req['active'], vToken['id_user'], datetime.date.today()))
        id = req['kelurahan_code']
        inc_db.addAuditMaster(con,idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
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
        UPDATE `sl_04_m_kelurahan` 
        SET 
            `description` = %s, 
            `kecamatan_code` = %s, 
            `update_by` = %s, 
            `update_date` = %s, 
            `active` = %s 
        WHERE 
           `kelurahan_code` = %s
        """
        cur.execute(sqlUpdate, ( req['description'], 
            req['kecamatan_code'], vToken['id_user'], datetime.date.today(), req['active'], id))
        inc_db.addAuditMaster(con,idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
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
    
    try:
        sqlDelete = """
        UPDATE `sl_04_m_kelurahan` 
        SET 
            delete_by = %s, 
            delete_date = %s, 
            delete_mark = 1 
        WHERE 
            kelurahan_code = %s
        """
        
        cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), id))
        inc_db.addAuditMaster(con,idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()
    

    cur.close()
    return inc_def.send_response_data("Deleted!", 200)