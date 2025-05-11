import json
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

#id menu cek table sc_02_menu
def get_id_menu():
    return '45'

def master_table_name():
    return 'st_03_tax_group_items'

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
        tg.tax_group_id,
        tg.name tax_group_name,
        tt.tax_type_id,
        tt.name tax_type_name
    FROM
        `st_03_tax_group_items` tgi
        LEFT JOIN `st_01_tax_groups` tg ON ( tg.tax_group_id = tgi.tax_group_id ) 
        LEFT JOIN `st_02_tax_types` tt ON ( tt.tax_type_id = tgi.tax_type_id ) 
    WHERE
        tgi.delete_mark = 0 
        AND tgi.active = 1
    """
    if params is not None:
        if params.get('id'):
            sql += " AND tgi.tax_group_id = " + params.get('id') 

    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        data = {
            "tax_group_id": row[0],
            "tax_group_name": row[1],
            "tax_type_id": row[2],
            "tax_type_name": row[3]
        }
        allData.append(data)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    for row in req['tax_type_id']:
        tax_type_id = row['id']
        tax_group_id = req['tax_group_id']

        sqlInsert = """
        INSERT INTO `st_03_tax_group_items` 
            ( tax_group_id, tax_type_id, active, create_by, create_date, delete_mark ) 
        VALUES 
            (%s, %s, %s, %s, %s, 0)
        """
        cur.execute(sqlInsert, (tax_group_id, tax_type_id, req['active'], vToken['id_user'], datetime.date.today()))
        id = req['tax_group_id']
        keterangan_audit_master = "id_master = tax_group_id; tax_type_id = " + str(tax_type_id)
        inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionCreate(), id, keterangan_audit_master, master_table_name(), vToken['id_user'])
        con.commit()
    
    sql = """
    SELECT
        tg.tax_group_id,
        tg.name tax_group_name,
        tt.tax_type_id,
        tt.name tax_type_name
    FROM
        `st_03_tax_group_items` tgi
        LEFT JOIN `st_01_tax_groups` tg ON ( tg.tax_group_id = tgi.tax_group_id ) 
        LEFT JOIN `st_02_tax_types` tt ON ( tt.tax_type_id = tgi.tax_type_id ) 
    WHERE
        tgi.delete_mark = 0 
        AND tgi.active = 1
        AND tgi.tax_group_id = %s
    """
    cur.execute(sql, (req['tax_group_id']))
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        data = {
            "tax_group_id": row[0],
            "tax_group_name": row[1],
            "tax_type_id": row[2],
            "tax_type_name": row[3]
        }
        allData.append(data)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    # Delete data with tax_group_id X
    sqlDelete = "DELETE FROM `st_03_tax_group_items` WHERE tax_group_id = " + id
    cur.execute(sqlDelete)

    # Insert new data (updated)
    for row in req['tax_type_id']:
        tax_type_id = row['id']
        tax_group_id = id

        sqlInsert = """
        INSERT INTO `st_03_tax_group_items` 
            ( tax_group_id, tax_type_id, active, create_by, create_date, delete_mark ) 
        VALUES 
            (%s, %s, %s, %s, %s, 0)
        """
        cur.execute(sqlInsert, (tax_group_id, tax_type_id, req['active'], vToken['id_user'], datetime.date.today()))
        keterangan_audit_master = "id_master = tax_group_id; tax_type_id = " + str(tax_type_id)
        inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionEdit(), id, keterangan_audit_master, master_table_name(), vToken['id_user'])
        con.commit()

    sql = """
    SELECT
        tg.tax_group_id,
        tg.name tax_group_name,
        tt.tax_type_id,
        tt.name tax_type_name
    FROM
        `st_03_tax_group_items` tgi
        LEFT JOIN `st_01_tax_groups` tg ON ( tg.tax_group_id = tgi.tax_group_id ) 
        LEFT JOIN `st_02_tax_types` tt ON ( tt.tax_type_id = tgi.tax_type_id ) 
    WHERE
        tgi.delete_mark = 0 
        AND tgi.active = 1
        AND tgi.tax_group_id = %s
    """
    
    cur.execute(sql, (id))
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        data = {
            "tax_group_id": row[0],
            "tax_group_name": row[1],
            "tax_type_id": row[2],
            "tax_type_name": row[3]
        }
        allData.append(data)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    sqlDelete = """
    UPDATE `st_03_tax_group_items` 
    SET 
        delete_by = %s, 
        delete_date = %s, 
        delete_mark = 1 
    WHERE 
        tax_group_id = %s
    """
    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), id))
    keterangan_audit_master = 'id_master = tax_group_id'
    inc_db.addAuditMaster(con, get_id_menu(), inc_def.getActionDelete(), id, keterangan_audit_master, master_table_name(), vToken['id_user'])
    con.commit()
    
    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)