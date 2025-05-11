import json
import datetime
import pymysql.cursors
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

tableName = 'in_04_t_stock_master_location'

def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 200)
    
    global idMenu
    
    httpMethod = event['httpMethod']
    headers = event['headers']
    user_token = headers.get('token')
    # url_path = headers.get('path')
    
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
    
    # idMenu = inc_db.getIdMenuByTableName(con, tableName)
    # dataMenu = inc_db.getDataMenu(con, idMenu)
    # pathMenunya = dataMenu['path']
    # pathFull = pathMenunya + "/" + pathActionnya
    
    if user_token is None:
        return inc_def.send_response_data("Access Denied", 403)
    else:
        vToken = valid_token.validationToken(user_token)
        if vToken == "false":
            return inc_def.send_response_data("Invalid Token", 404)
        else:
            # vMenu = valid_menu.validationMenu(vToken['id_user'], url_path)
            # if vMenu == "false":
            #     return inc_def.send_response_data("Forbidden access for this menu", 403)
            # else:
            #     if url_path != pathFull:
            #         return inc_def.send_response_data("Path menu action doesn't match", 403)
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
    headers = event['headers']
    inKodePerusahaan = headers.get('kode_perusahaan')

    allData = []
    try:
        sql = """
        SELECT
            stock_code,
            kode_lokasi,
        	actual_cost,
        	last_cost,
        	material_cost,
        	labour_cost,
        	overhead_cost
        FROM
        	in_04_t_stock_master_location
        WHERE 
        """
        sql += " kode_perusahaan = '" + inKodePerusahaan + "'"
        
        if params is not None:
            if params.get('stock_code'):
                sql += " AND `stock_code` = '" + params.get('stock_code') + "'"
            if params.get('kode_lokasi'):
                sql += " AND `kode_lokasi` = '" + params.get('kode_lokasi') + "'"

        cur.execute(sql)
        myresult = cur.fetchall()

        for row in myresult:
            data = {
                "stock_code": row['stock_code'],
                "kode_lokasi": row['kode_lokasi'],
                "actual_cost": row['actual_cost'],
                "last_cost": row['last_cost'],
                "material_cost": row['material_cost'],
                "labour_cost": row['labour_cost'],
                "overhead_cost": row['overhead_cost'],
                
            }
            allData.append(data)
    except Exception as e:
        con.close()
        return inc_def.send_response_data(str(e), 500)

    cur.close()
    return inc_def.send_response_data(allData, 200)
