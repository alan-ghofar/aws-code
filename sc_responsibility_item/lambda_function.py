import json
import datetime
import pymysql.cursors
import valid_token as v_token
import valid_menu as v_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

# id menu cek table sc_02_menu
tableName = 'sc_04_responsibility_item'


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
        rei.id,
        rei.id_resp,
        re.nama_resp,
        rei.id_role,
        ro.nama_role,
        rei.id_menu,
        me.nama_menu,
        me.keterangan,
        me.path 
    FROM
        `sc_04_responsibility_item` rei
        LEFT JOIN `sc_01_responsibility` re ON ( re.id = rei.id_resp )
        LEFT JOIN `sc_01_role` ro ON ( ro.id = rei.id_role )
        LEFT JOIN `sc_02_menu` me ON ( me.id = rei.id_menu ) 
    WHERE
        rei.delete_mark = 0 
    """
    if params is not None:
        if params.get('id'):
            sql += " AND rei.id =" + params.get('id')

    sql += """
    GROUP BY
        rei.id_resp,
        rei.id_role,
        rei.id_menu 
    ORDER BY
        re.nama_resp,
        ro.nama_role,
        me.nama_menu
    """

    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        sqlAction = """
        SELECT
            rei.id,
            rei.id_menu_action,
            ma.id_action,
            ac.nama_action,
            rei.active
        FROM
            `sc_04_responsibility_item` rei
            LEFT JOIN `sc_03_menu_action` ma ON ( ma.id = rei.id_menu_action )
            LEFT JOIN `sc_01_action` ac ON ( ac.id = ma.id_action ) 
        WHERE
            rei.delete_mark = 0 
            AND rei.id_resp = %s
            AND rei.id_role = %s
            AND rei.id_menu = %s
        """
        cur.execute(sqlAction, (row['id_resp'], row['id_role'], row['id_menu']))
        resAction = cur.fetchall()
        dataAction = []
        for rowAct in resAction:
            action = {
                "id_resp_item": rowAct['id'],
                "id_menu_action": rowAct['id_menu_action'],
                "id_action": rowAct['id_action'],
                "nama_action": rowAct['nama_action'],
                "active": rowAct['active'],
            }
            dataAction.append(action)

        resp_item = {
            "id": row['id'],
            "id_resp": row['id_resp'],
            "nama_resp": row['nama_resp'],
            "id_role": row['id_role'],
            "nama_role": row['nama_role'],
            "id_menu": row['id_menu'],
            "nama_menu": row['nama_menu'],
            "keterangan": row['keterangan'],
            # "id_menu_action": row['id_menu_action'],
            # "id_action": row['id_action'],
            # "nama_action": row['nama_action'],
            # "path": row['path'] + "/" + row['nama_action'],
            "actions": dataAction
        }
        allData.append(resp_item)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    try:
        for row in req['menu_action']:
            id_menu_action = row['id_menu_action']
            active = row['active']
            sqlInsert = """
            INSERT INTO `sc_04_responsibility_item` 
                ( id_resp, id_role, id_menu, id_menu_action, active, create_by, create_date, delete_mark ) 
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, 0)
            """
            cur.execute(sqlInsert, (
                req['id_resp'], req['id_role'], req['id_menu'], id_menu_action, active,
                vToken['id_user'], datetime.date.today())
            )
        keterangan = "Insert data resp item dengan data: id_resp=" + req['id_resp'] + "; id_role=" + req['id_role'] + "; id_menu=" + req['id_menu']
        master = "id_resp=" + req['id_resp'] + ";id_role=" + req['id_role'] + ";id_menu=" + req['id_menu']
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), master, keterangan, tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur.close()
    return inc_def.send_response_data("Success", 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']

    try:
        for row in req['menu_action']:
            id_resp_item = row['id_resp_item']
            id_menu_action = row['id_menu_action']
            active = row['active']
            
            if id_resp_item == 0 :
                sqlInsert = """
                INSERT INTO `sc_04_responsibility_item` 
                    ( id_resp, id_role, id_menu, id_menu_action, active, create_by, create_date, delete_mark ) 
                VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, 0)
                """
                cur.execute(sqlInsert, (
                    req['id_resp'], req['id_role'], req['id_menu'], id_menu_action, active,
                    vToken['id_user'], datetime.date.today())
                )
            else:
                sqlUpdate = """
                UPDATE `sc_04_responsibility_item` 
                SET 
                    id_resp=%s, 
                    id_role=%s, 
                    id_menu=%s, 
                    id_menu_action=%s, 
                    active=%s, 
                    update_by=%s, 
                    update_date=%s
                WHERE 
                    id=%s
                """
                cur.execute(sqlUpdate, (req['id_resp'], req['id_role'], req['id_menu'], id_menu_action, active,
                            vToken['id_user'], datetime.date.today(), id_resp_item))
        keterangan = "Update data resp item dengan data: id_resp=" + req['id_resp'] + "; id_role=" + req['id_role'] + "; id_menu=" + req['id_menu']
        master = "id_resp=" + req['id_resp'] + ";id_role=" + req['id_role'] + ";id_menu=" + req['id_menu']
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), master, keterangan, tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur.close()
    return inc_def.send_response_data("Success", 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    params = event['queryStringParameters']
    vToken = event['data_user']
    id_resp = params['id_resp']
    id_role = params['id_role']
    id_menu = params['id_menu']

    sqlUpdate = """
    UPDATE 
        `sc_04_responsibility_item` 
    SET 
        delete_by=%s, 
        delete_date=%s, 
        delete_mark=%s 
    WHERE 
        id_resp = %s
        AND id_role = %s
        AND id_menu = %s 
    """
    cur.execute(sqlUpdate, (
        vToken['id_user'], datetime.date.today(), "1",
        id_resp, id_role, id_menu
    ))
    keterangan = "Hapus data resp item dengan data: id_resp=" + id_resp + "; id_role=" + id_role + "; id_menu=" + id_menu
    master = "id_resp=" + id_resp + "; id_role=" + id_role + "; id_menu=" + id_menu
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), master, keterangan, tableName, vToken['id_user'])
    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
