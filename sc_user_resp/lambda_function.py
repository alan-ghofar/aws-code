import json
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

#id menu cek table sc_02_menu
def get_id_menu():
    return '55'

#nama table nya
def master_table_name():
    return 'sc_02_user_resp'
    
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

    sql = """
    SELECT
        ures.id,
        ures.id_user,
        ures.id_responsibility,
        u.nama_user,
        u.nama_lengkap,
        r.nama_resp,
        r.keterangan,
        ures.kode_perusahaan,
        p.nama_perusahaan 
    FROM
        `sc_02_user_resp` ures
        LEFT JOIN `sc_01_responsibility` r ON ( r.id = ures.id_responsibility )
        LEFT JOIN `sc_01_user` u ON ( u.id = ures.id_user )
        LEFT JOIN `st_01_perusahaan` p ON ( p.kode_perusahaan = ures.kode_perusahaan ) 
    WHERE
        ures.delete_mark = 0
    """
    if params is not None:
        if params.get('id'):
            sql += " AND ures.id =" + params.get('id')

    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        menu = {
            "id": row[0],
            "id_user": row[1],
            "id_responsibility": row[2],
            "nama_user": row[3],
            "nama_lengkap": row[4],
            "nama_resp": row[5],
            "keterangan": row[6],
            "kode_perusahaan": row[7],
            "nama_perusahaan": row[8],
        }
        allData.append(menu)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    sqlInsert = "INSERT INTO `sc_02_user_resp` ( id_user, id_responsibility, kode_perusahaan, expired_date, create_by, create_date, delete_mark) VALUES (%s, %s, %s, %s, %s, %s, 0)"
    cur.execute(sqlInsert, (req['id_user'], req['id_responsibility'], req['kode_perusahaan'],
                req['expired_date'], vToken['id_user'], datetime.date.today()))
    id = con.insert_id()
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionCreate(), id, '', master_table_name(), vToken['id_user'])
    con.commit()
    
    sql = """
    SELECT
        ures.id,
        ures.id_user,
        ures.id_responsibility,
        u.nama_user,
        u.nama_lengkap,
        r.nama_resp,
        r.keterangan,
        ures.kode_perusahaan,
        p.nama_perusahaan 
    FROM
        `sc_02_user_resp` ures
        LEFT JOIN `sc_01_responsibility` r ON ( r.id = ures.id_responsibility )
        LEFT JOIN `sc_01_user` u ON ( u.id = ures.id_user )
        LEFT JOIN `st_01_perusahaan` p ON ( p.kode_perusahaan = ures.kode_perusahaan ) 
    WHERE
        ures.delete_mark = 0
        AND ures.id = %s
    """
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "id_user": row[1],
        "id_responsibility": row[2],
        "nama_user": row[3],
        "nama_lengkap": row[4],
        "nama_resp": row[5],
        "keterangan": row[6],
        "kode_perusahaan": row[7],
        "nama_perusahaan": row[8],
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
    sqlUpdate = "UPDATE `sc_02_user_resp` SET id_user=%s, id_responsibility=%s, kode_perusahaan=%s, expired_date=%s, update_by=%s, update_date=%s WHERE id=%s"
    cur.execute(sqlUpdate, (req['id_user'], req['id_responsibility'], req['kode_perusahaan'],
                req['expired_date'], vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionEdit(), id, '', master_table_name(), vToken['id_user'])
    con.commit()

    sql = """
    SELECT
        ures.id,
        ures.id_user,
        ures.id_responsibility,
        u.nama_user,
        u.nama_lengkap,
        r.nama_resp,
        r.keterangan,
        ures.kode_perusahaan,
        p.nama_perusahaan 
    FROM
        `sc_02_user_resp` ures
        LEFT JOIN `sc_01_responsibility` r ON ( r.id = ures.id_responsibility )
        LEFT JOIN `sc_01_user` u ON ( u.id = ures.id_user )
        LEFT JOIN `st_01_perusahaan` p ON ( p.kode_perusahaan = ures.kode_perusahaan ) 
    WHERE
        ures.delete_mark = 0
        AND ures.id = %s
    """
    cur.execute(sql, (params['id']))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "id_user": row[1],
        "id_responsibility": row[2],
        "nama_user": row[3],
        "nama_lengkap": row[4],
        "nama_resp": row[5],
        "keterangan": row[6],
        "kode_perusahaan": row[7],
        "nama_perusahaan": row[8],
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']
    sqlUpdate = "UPDATE `sc_02_user_resp` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE id=%s"
    cur.execute(sqlUpdate, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionDelete(), id, '', master_table_name(), vToken['id_user'])
    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
