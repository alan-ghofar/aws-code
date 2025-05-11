import json
import datetime
import pymysql.cursors
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

#id menu cek table sc_02_menu
def get_id_menu():
    return '24'

def master_table_name():
    return 'st_03_bahasa_used'
    
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
    cur = con.cursor(pymysql.cursors.DictCursor)
    # language_def = data_sys_prefs.getValueByName('language_default', cur)
    params = event['queryStringParameters']

    sql = """
    SELECT
        bu.id,
        bu.kode_bahasa,
        bu.modul,
        bu.nilai,
        bd.id def_id,
        bd.modul def_modul,
        bd.nilai def_nilai 
    FROM
        st_03_bahasa_used bu
        LEFT JOIN st_02_bahasa_def bd ON ( bd.id = bu.id_bahasa_def ) 
    WHERE
        bu.`delete_mark` = 0
    """
    if params is not None:
        if params.get('id'):
            sql += " AND bu.id = " + params.get('id')
        if params.get('kode_bahasa'):
            sql += " AND bu.kode_bahasa = '" + params.get('kode_bahasa') + "'"
        if params.get('modul'):
            sql += " AND (bu.modul = '" + params.get('modul') + "' OR bu.modul = 'global' )"
            
    sql += """
    ORDER BY
        bu.modul
    """

    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        data = {
            "id": row["id"],
            "kode_bahasa": row["kode_bahasa"],
            "modul": row["modul"],
            "nilai": row["nilai"],
            "def_id": row["def_id"],
            "def_modul": row["def_modul"],
            "def_nilai": row["def_nilai"],
        }
        allData.append(data)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    sqlInsert = "INSERT INTO `st_03_bahasa_used` ( kode_bahasa, id_bahasa_def, modul, nilai, create_by, create_date, delete_mark ) VALUES (%s, %s, %s, %s, %s, %s, 0)"
    cur.execute(sqlInsert, (req['kode_bahasa'], req['id_bahasa_def'],
                req['modul'], req['nilai'], vToken['id_user'], datetime.date.today()))
    id = con.insert_id()
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionCreate(), id, '', master_table_name(), vToken['id_user'])
    con.commit()
    
    sql = "SELECT id, kode_bahasa, modul, nilai FROM `st_03_bahasa_used` WHERE id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "kode_bahasa": row[1],
        "modul": row[2],
        "nilai": row[3]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']
    sqlUpdate = "UPDATE `st_03_bahasa_used` SET `kode_bahasa` = %s, id_bahasa_def = %s, modul = %s, `nilai` = %s, `update_by` = %s, `update_date` = %s WHERE id=%s"
    cur.execute(sqlUpdate, (req['kode_bahasa'], req['id_bahasa_def'], req['modul'],
                req['nilai'], vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionEdit(), id, '', master_table_name(), vToken['id_user'])
    con.commit()

    sql = "SELECT id, kode_bahasa, modul, nilai FROM `st_03_bahasa_used` WHERE id = %s"
    cur.execute(sql, (params['id']))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "kode_bahasa": row[1],
        "modul": row[2],
        "nilai": row[3]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']
    sqlUpdate = "UPDATE `st_03_bahasa_used` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE id=%s"
    cur.execute(sqlUpdate, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionDelete(), id, '', master_table_name(), vToken['id_user'])
    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
