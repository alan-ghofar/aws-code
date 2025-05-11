import json
import datetime
import hashlib
import valid_token
import valid_menu
import database_connection as db_con 
import includes_definitions as inc_def 
import includes_db_general as inc_db 

tableName = 'pr_04_m_chart_suppl_type'

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
    # return inc_def.send_response_data(pathFull, 403)

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
        cst.chart_suppl_type_id,
        cst.supplier_type_id,
        cst.purchase_account,
        cst.payable_account,
        cst.purchase_diskon,
        cst.deposit_account
    FROM 
        `pr_04_m_chart_suppl_type` cst
        LEFT JOIN `pr_01_m_supplier_type` mst ON ( mst.supplier_type_id = cst.supplier_type_id )
        LEFT JOIN `fi_03_m_chart_of_account` mcoa ON ( mcoa.account_code = cst.purchase_account AND mcoa.account_code = cst.payable_account AND mcoa.account_code = cst.purchase_diskon AND mcoa.account_code = cst.deposit_account ) 
    WHERE
        cst.delete_mark = 0
        AND cst.active = 1
    """
    if params is not None:
        if params.get('id'):
            sql += " AND cst.chart_suppl_type_id = '" + params.get('id') + "'"
    
    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        menu = {
            "chart_suppl_type_id": row[0],
            "supplier_type_id": row[1],
            "purchase_account": row[2],
            "payable_account": row[3],
            "purchase_diskon": row[4],
            "deposit_account": row[5],
            # "create_by": row[6],
            # "create_date": row[7],
            # "update_by": row[8],
            # "update_date": row[9],
            # "delete_by": row[10],
            # "delete_date": row[11],
            # "active": row[12],
            # "delete_mark": row[13],
        }
        allData.append(menu)

    cur.close()
    return inc_def.send_response_data(allData, 200)

# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    sqlInsert = """INSERT INTO `pr_04_m_chart_suppl_type` (  supplier_type_id, purchase_account, payable_account, purchase_diskon, 
    deposit_account, create_by, create_date, active, delete_mark) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, 1, 0)"""
    cur.execute(sqlInsert, (str(req['supplier_type_id']), req['purchase_account'], req['payable_account'], 
    req['purchase_diskon'], req['deposit_account'], str(vToken['id_user']), datetime.date.today()))
    id = con.insert_id()
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])

    sql = "SELECT * FROM `pr_04_m_chart_suppl_type` WHERE chart_suppl_type_id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "chart_suppl_type_id": row[0],
        "supplier_type_id": row[1],
        "purchase_account": row[2],
        "payable_account": row[3],
        "purchase_diskon": row[4],
        "deposit_account": row[5]
    }
    con.commit()
    cur.close()
    return inc_def.send_response_data(data, 200)

# =============================== FUNCTION PUT
def functionPut(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']
    
    sqlUpdate = """
    UPDATE `pr_04_m_chart_suppl_type` SET supplier_type_id=%s, purchase_account=%s, payable_account=%s, purchase_diskon=%s,
    deposit_account=%s, update_by=%s, update_date=%s, active=%s WHERE chart_suppl_type_id=%s
    """
    cur.execute(sqlUpdate, (req['supplier_type_id'], req['purchase_account'], req['payable_account'], req['purchase_diskon'], req['deposit_account'], vToken['id_user'], datetime.date.today(), req['active'], id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    con.commit()
    sql = "SELECT * FROM `pr_04_m_chart_suppl_type` WHERE chart_suppl_type_id=%s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "chart_suppl_type_id": row[0],
        "supplier_type_id": row[1],
        "purchase_account": row[2],
        "payable_account": row[3],
        "purchase_diskon": row[4],
        "deposit_account": row[5],
        "active": row[8]
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

    sqlUpdate = """
    UPDATE `pr_04_m_chart_suppl_type` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE chart_suppl_type_id=%s
    """
    cur.execute(sqlUpdate, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    data = {
        "message": "Deleted!"
    }
    con.commit()
    cur.close()
    return inc_def.send_response_data(data, 200)
