import json
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

#id menu cek table sc_02_menu
def get_id_menu():
    return '14'

def master_table_name():
    return 'sc_01_responsibility'

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

    sql = "SELECT * FROM `sc_01_responsibility` WHERE `delete_mark`=0 AND `active`=1"
    if params is not None:
        if params.get('id'):
            sql += " AND id =" + params.get('id')

    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        sqlRole = """
        SELECT
            rei.id_role,
            ro.nama_role 
        FROM
            `sc_04_responsibility_item` rei
            LEFT JOIN `sc_01_role` ro ON ( ro.id = rei.id_role ) 
        WHERE
            rei.id_resp = %s 
        GROUP BY
            rei.id_role
        """
        cur.execute(sqlRole, (row[0]))
        resRole = cur.fetchall()

        allRole = []
        for rowRole in resRole:
            sqlMenu = """
            SELECT
                rei.id_menu,
                me.nama_menu,
                me.keterangan,
                me.path 
            FROM
                `sc_04_responsibility_item` rei
                LEFT JOIN `sc_02_menu` me ON ( me.id = rei.id_menu ) 
            WHERE
                rei.id_resp = %s 
            GROUP BY
                rei.id_menu
            """
            cur.execute(sqlMenu, (row[0]))
            resMenu = cur.fetchall()

            allMenu = []
            for rowMenu in resMenu:
                sqlAction = """
                SELECT
                    ma.id_action,
                    ac.nama_action,
                    ma.params 
                FROM
                    `sc_04_responsibility_item` rei
                    LEFT JOIN `sc_03_menu_action` ma ON ( ma.id = rei.id_menu_action )
                    LEFT JOIN `sc_01_action` ac ON ( ac.id = ma.id_action ) 
                WHERE
                    rei.id_resp = %s 
                GROUP BY
                    ma.id_action
                """
                cur.execute(sqlAction, (row[0]))
                resAction = cur.fetchall()

                allAction = []
                for rowAction in resAction:
                    action = {
                        "id_action": rowAction[0],
                        "nama_action": rowAction[1],
                        "params": rowAction[2],
                        "path": rowMenu[3] + "/" + rowAction[1] if rowAction[2] is None else rowMenu[3] + "/" + rowAction[1] + "/:" + rowAction[2]
                    }
                    allAction.append(action)

                menu = {
                    "id_menu": rowMenu[0],
                    "nama_menu": rowMenu[1],
                    "keterangan": rowMenu[2],
                    "path": rowMenu[3],
                    "action": allAction
                }
                allMenu.append(menu)

            role = {
                "id_role": rowRole[0],
                "nama_role": rowRole[1],
                "menu": allMenu
            }
            allRole.append(role)

        data = {
            "id": row[0],
            "nama_resp": row[1],
            "keterangan": row[2],
            "role": allRole
        }
        allData.append(data)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    sqlInsert = "INSERT INTO `sc_01_responsibility` ( nama_resp, keterangan, create_by, create_date, active, delete_mark ) VALUES (%s, %s, %s, %s, 1, 0)"
    cur.execute(sqlInsert, (req['nama_resp'], req['keterangan'],
                vToken['id_user'], datetime.date.today()))
    id = con.insert_id()
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionCreate(), id, '', master_table_name(), vToken['id_user'])
    con.commit()
    
    sql = "SELECT id, nama_resp, keterangan FROM `sc_01_responsibility` WHERE id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "nama_resp": row[1],
        "keterangan": row[2]
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
    sqlUpdate = "UPDATE `sc_01_responsibility` SET nama_resp=%s, keterangan=%s, update_by=%s, update_date=%s, active=%s WHERE id=%s"
    cur.execute(sqlUpdate, (req['nama_resp'], req['keterangan'],
                vToken['id_user'], datetime.date.today(), req['active'], id))
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionEdit(), id, '', master_table_name(), vToken['id_user'])
    con.commit()

    sql = "SELECT id, nama_resp, keterangan FROM `sc_01_responsibility` WHERE id = %s"
    cur.execute(sql, (params['id']))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "nama_resp": row[1],
        "keterangan": row[2]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']
    sqlUpdate = "UPDATE `sc_01_responsibility` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE id=%s"
    cur.execute(sqlUpdate, (vToken['id_user'],
                datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionDelete(), id, '', master_table_name(), vToken['id_user'])
    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
