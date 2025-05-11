import json
import datetime
import hashlib
import valid_token
import valid_menu
import database_connection as db_con 
import includes_definitions as inc_def 
import includes_db_general as inc_db 

tableName = 'pr_03_m_purchase_addtional_fee'

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
    cur = con.cursor()
    params = event['queryStringParameters']

    sql = """
    SELECT 
        paf.purch_addtional_fee_id,
        paf.supplier_id,
        paf.stock_code,
        paf.is_using_tax,
        paf.is_include_hpp
    FROM 
        `pr_03_m_purchase_addtional_fee` paf
        LEFT JOIN `pr_02_m_supplier` tg ON ( tg.supplier_id = paf.supplier_id )
        LEFT JOIN `in_03_m_stock_master` sm ON ( sm.stock_code = paf.stock_code )
    WHERE
        paf.delete_mark = 0
        AND tg.delete_mark = 0
        AND paf.active = 1
        AND sm.delete_mark = 0
    """
    if params is not None:
        if params.get('id'):
            sql += " AND paf.purch_addtional_fee_id =" + params.get('id')
    
    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        menu = {
            "purch_addtional_fee_id": row[0],
            "supplier_id": row[1],
            "stock_code": row[2],
            "is_using_tax": row[3],
            "is_include_hpp": row[4],
            # "create_by": row[5],
            # "create_date": row[6],
            # "update_by": row[7],
            # "update_date": row[8],
            # "delete_by": row[9],
            # "delete_date": row[10],
            # "active": row[11],
            # "delete_mark": row[12],
        }
        allData.append(menu)

    cur.close()
    return inc_def.send_response_data(allData, 200)

# =============================== FUNCTION POST
def functionPost(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']
    
    field = {
        "supplier_id": req['supplier_id'],
        "stock_code": req['stock_code']
    }

    isDouble = inc_db.checkDoubleData(cur, field, tableName)

    if isDouble == "true" : 
        return inc_def.send_response_data("Data is exist", 400)
        
    try:
    
        sqlInsert = """INSERT INTO `pr_03_m_purchase_addtional_fee` (  supplier_id, stock_code, is_using_tax, is_include_hpp, create_by, create_date, active, delete_mark) 
        VALUES (%s, %s, %s, %s, %s, %s, 1, 0)"""
        cur.execute(sqlInsert, (req['supplier_id'], req['stock_code'], req['is_using_tax'], req['is_include_hpp'], vToken['id_user'], datetime.date.today()))
        id = con.insert_id()
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()
        
    sql = "SELECT * FROM `pr_03_m_purchase_addtional_fee` WHERE purch_addtional_fee_id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "purch_addtional_fee_id": row[0],
        "supplier_id": row[1],
        "stock_code": row[2],
        "is_using_tax": row[3],
        "is_include_hpp": row[4]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)

# =============================== FUNCTION PUT
def functionPut(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']
    
    try:

        sqlUpdate = """
        UPDATE `pr_03_m_purchase_addtional_fee` SET supplier_id=%s, stock_code=%s, is_using_tax=%s, is_include_hpp=%s, update_by=%s, update_date=%s, active=%s WHERE purch_addtional_fee_id=%s
        """
        cur.execute(sqlUpdate, (req['supplier_id'], req['stock_code'], req['is_using_tax'], req['is_include_hpp'], vToken['id_user'], datetime.date.today(), req['active'], id))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_det.send_response_data(str(e), 500)
    else:
        con.commit()
        
    sql = "SELECT * FROM `pr_03_m_purchase_addtional_fee` WHERE purch_addtional_fee_id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "purch_addtional_fee_id": row[0],
        "supplier_id": row[1],
        "stock_code": row[2],
        "is_using_tax": row[3],
        "is_include_hpp": row[4],
        "active": row[11]
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
    UPDATE `pr_03_m_purchase_addtional_fee` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE purch_addtional_fee_id=%s
    """   
    cur.execute(sqlUpdate, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    data = {
        "message": "Deleted!"
    }
    con.commit()
    cur.close()
    return inc_def.send_response_data(data, 200)
    