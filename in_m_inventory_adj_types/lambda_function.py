import json
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

tableName = 'in_01_m_inventory_adj_types'

def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 200)
    
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
    dataMenu = inc_db.getDataMenu(con, idMenu)
    pathMenunya = dataMenu['path'] #inc_db.getPathMenu(con, idMenu)
    
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
        SELECT `id`, `adjustment_type`, `adj_positive`, `adj_negative`, `coa_positive`, `coa_negative` 
        FROM `in_01_m_inventory_adj_types` 
        WHERE `active` = 1 AND `delete_mark` = 0
        AND id LIKE %s 
        AND adj_positive LIKE %s
        AND adj_negative LIKE %s
        """
        
        filterId ='%'
        filterAdjPositive = '%'
        filterAdjNegative = '%'
        if params is not None:
            if params.get('id'):
                filterId = params.get('id')
            if params.get('adj_positive'):
                filterAdjPositive = params.get('adj_positive')
            if params.get('adj_negative'):
                filterAdjNegative = params.get('adj_negative')
    
        cur.execute(sql, (filterId, filterAdjPositive, filterAdjNegative))
        myresult = cur.fetchall()
    
        allData = []
        for row in myresult:
            data = {
                "id": row[0],
                "adjustment_type": row[1],
                "adj_positive": row[2],
                "adj_negative": row[3],
                "coa_positive": row[4],
                "coa_negative": row[5]
            }
            allData.append(data)
    except Exception as e:
        return inc_def.send_response_data(str(e), 500)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    sqlInsert = """
    INSERT INTO `in_01_m_inventory_adj_types` 
        ( `adjustment_type`, `adj_positive`, `adj_negative`, `coa_positive`, `coa_negative`, `active`, `create_by`, `create_date`, `delete_mark` ) 
    VALUES 
        (%s, %s, %s, %s, %s, %s, %s, %s, 0)
    """
    cur.execute(sqlInsert, (req['adjustment_type'], req['adj_positive'], req['adj_negative'], req['coa_positive'], req['coa_negative'], req['active'], vToken['id_user'], datetime.date.today()))
    id = con.insert_id()
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    con.commit()
    

    sql = "SELECT id, adjustment_type, adj_positive, adj_negative, coa_positive, coa_negative FROM `in_01_m_inventory_adj_types` WHERE id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "adjustment_type": row[1],
        "adj_positive": row[2],
        "adj_negative": row[3],
        "coa_positive": row[4],
        "coa_negative": row[5]
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
    sqlUpdate = """
    UPDATE `in_01_m_inventory_adj_types` 
    SET 
        `adjustment_type` = %s, 
        `adj_positive` = %s, 
        `adj_negative` = %s, 
        `coa_positive` = %s, 
        `coa_negative` = %s, 
        `update_by` = %s, 
        `update_date` = %s, 
        `active` = %s 
    WHERE 
       `id` = %s
    """
    cur.execute(sqlUpdate, (req['adjustment_type'], req['adj_positive'], req['adj_negative'], req['coa_positive'], req['coa_negative'], vToken['id_user'], datetime.date.today(), req['active'], id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    con.commit()

    sql = "SELECT id, adjustment_type, adj_positive, adj_negative, coa_positive, coa_negative FROM `in_01_m_inventory_adj_types` WHERE id = %s"
    cur.execute(sql, (params['id']))
    row = cur.fetchone()
    data = {
        "id": row[0],
        "adjustment_type": row[1],
        "adj_positive": row[2],
        "adj_negative": row[3],
        "coa_positive": row[4],
        "coa_negative": row[5]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    sqlUpdate = "UPDATE `in_01_m_inventory_adj_types` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE id = %s"
    cur.execute(sqlUpdate, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    con.commit()
    
    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
