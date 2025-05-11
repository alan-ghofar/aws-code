import json
import datetime
import hashlib
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db
    
tableName = 'sc_01_user'

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
    cur = con.cursor()
    params = event['queryStringParameters']

    try:
        sql = """
        SELECT
            id,
            nama_user,
            nama_lengkap,
            phone,
            expired_date,
            email,
            `language`
        FROM
            `sc_01_user` 
        WHERE
            delete_mark = 0 
        """
        
        showInactive = "false"
        if params is not None:
            if params.get('id'):
                sql += " AND id =" + params.get('id')
                
            if params.get('show_inactive') and params.get('show_inactive') == 0 :
                showInactive = "true"
        
        if showInactive == "false":
            sql += " AND active = 1"
        
        cur.execute(sql)
        myresult = cur.fetchall()
    
        allData = []
        for row in myresult:
            menu = {
                "id": row[0],
                "nama_user": row[1],
                "nama_lengkap": row[2],
                "phone": row[3],
                # "expired_date": row[4],
                "email": row[5],
                "language": row[6],
                # "create_by": row[7],
                # "create_date": row[8],
                # "update_by": row[9],
                # "update_date": row[10],
                # "delete_by": row[11],
                # "delete_date": row[12],
                # "active": row[13],
                # "delete_mark": row[14],
    
            }
            allData.append(menu)
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
        idUser = getMaxId(cur) + 1
        
        sqlInsert = """INSERT INTO `sc_01_user` 
        ( id, nama_user, nama_lengkap, phone, password, expired_date, email, language, create_by, create_date, active, delete_mark) 
        VALUES 
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, 0)"""
        cur.execute(sqlInsert, (idUser, req['nama_user'], req['nama_lengkap'], req['phone'], hashlib.md5(req['password'].encode('utf-8')).hexdigest(), req['expired_date'], req['email'], req['language'], vToken['id_user'], datetime.date.today()))
        # id = con.insert_id()
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), idUser, '', tableName, vToken['id_user'])
        
        # INSERT USER PREF
        sqlInsertUserPref = """
        INSERT INTO 
            st_02_user_prefs 
            (id_user, kode, nilai, keterangan, active, delete_mark)
        SELECT 
            %s, kode, nilai, keterangan, 1, 0
        FROM 
            st_01_sys_prefs
        WHERE 
            for_user = 1 
            AND active = 1 
            AND delete_mark = 0;
        """
        cur.execute(sqlInsertUserPref, (str(idUser)))
        
        # INSERT KE USER PERUSAHAAN
        sqlInsertUserPerusahaan = """
        INSERT INTO `sc_02_user_perusahaan` 
            ( id_user, kode_perusahaan, default_perusahaan, create_by, create_date, delete_mark) 
        VALUES 
            (%s, %s, %s, %s, %s, 0)
        """
        cur.execute(sqlInsertUserPerusahaan, (idUser, req['kode_perusahaan'], req['kode_perusahaan'], vToken['id_user'], datetime.date.today()))
        
        # INSERT KE USER RESPONSIBILTY
        sqlInsertUserResponsibility = """
        INSERT INTO `sc_02_user_resp` 
            ( id_user, id_responsibility, kode_perusahaan, expired_date, create_by, create_date, delete_mark) 
        VALUES 
            (%s, %s, %s, %s, %s, %s, 0)
        """
        cur.execute(sqlInsertUserResponsibility, (idUser, req['id_responsibility'], req['kode_perusahaan'], req['expired_date'], vToken['id_user'], datetime.date.today()))
        
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
    cur = con.cursor()
    req = json.loads(event['body'])
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']

    sqlUpdate = "UPDATE `sc_01_user` SET  nama_lengkap=%s, phone=%s, expired_date=%s, language=%s, update_by=%s, update_date=%s, active=%s WHERE id=%s"
    cur.execute(sqlUpdate, (req['nama_lengkap'], req['phone'], req['expired_date'],
                req['language'], vToken['id_user'], datetime.date.today(), req['active'], id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    con.commit()
    sql = "SELECT * FROM `sc_01_user` WHERE id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "nama_user": row[1],
        "nama_lengkap": row[2],
        "phone": row[7]
    }
    
    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    #req = json.loads(event['body'])
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']

    sqlUpdate = "UPDATE `sc_01_user` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE id=%s"
    cur.execute(sqlUpdate, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    data = {
        "message": "Deleted!"
    }
    con.commit()
    cur.close()
    return inc_def.send_response_data(data, 200)
    

def getMaxId(cur):
    sql = "SELECT IFNULL(MAX(id),0) max FROM `sc_01_user`"
    cur.execute(sql)
    row = cur.fetchone()
    return row[0]
