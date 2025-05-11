import json
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

#id menu cek table sc_02_menu
def get_id_menu():
    return '21'

def master_table_name():
    return 'st_01_perusahaan'

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

    sql = "SELECT kode_perusahaan, nama_perusahaan FROM `st_01_perusahaan` WHERE `delete_mark`=0 AND `active`=1"
    if params is not None:
        if params.get('id'):
            sql += " AND kode_perusahaan LIKE '%" + params.get('id') + "%'"

    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        data = {
            "kode_perusahaan": row[0],
            "nama_perusahaan": row[1]
        }
        allData.append(data)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    sqlInsert = "INSERT INTO `st_01_perusahaan` ( kode_perusahaan, nama_perusahaan, create_by, create_date, active, delete_mark ) VALUES (%s, %s, %s, %s, 1, 0)"
    cur.execute(sqlInsert, (req['kode_perusahaan'], req['nama_perusahaan'],
                vToken['id_user'], datetime.date.today()))
    id = req['kode_perusahaan']
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionCreate(), id, '', master_table_name(), vToken['id_user'])
    con.commit()

    sql = "SELECT kode_perusahaan, nama_perusahaan FROM `st_01_perusahaan` WHERE kode_perusahaan = %s"
    cur.execute(sql, (req['kode_perusahaan']))
    row = cur.fetchone()
    data = {
        "kode_perusahaan": row[0],
        "nama_perusahaan": row[1]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    sqlUpdate = "UPDATE `st_01_perusahaan` SET  `nama_perusahaan` = %s, `update_by` = %s, `update_date` = %s, `active` = %s WHERE kode_perusahaan=%s"
    cur.execute(sqlUpdate, (req['nama_perusahaan'],
                vToken['id_user'], datetime.date.today(), req['active'], id))
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionEdit(), id, '', master_table_name(), vToken['id_user'])
    con.commit()

    sql = "SELECT kode_perusahaan, nama_perusahaan FROM `st_01_perusahaan` WHERE kode_perusahaan = %s"
    cur.execute(sql, (params['id']))
    row = cur.fetchone()
    data = {
        "kode_perusahaan": row[0],
        "nama_perusahaan": row[1]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    sqlUpdate = "UPDATE `st_01_perusahaan` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE kode_perusahaan=%s"
    cur.execute(sqlUpdate, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionDelete(), id, '', master_table_name(), vToken['id_user'])
    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
