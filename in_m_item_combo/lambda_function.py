import json
import datetime
import valid_token
import valid_menu
import pymysql.cursors
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

tableName = 'in_04_m_item_combo'

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
    dataMenu = inc_db.getDataMenu(con, idMenu)
    pathMenunya = dataMenu['path']
    
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
    # cur = con.cursor()
    cur = con.cursor(pymysql.cursors.DictCursor)
    params = event['queryStringParameters']
    headers = event['headers']
    inKodePerusahaan = headers.get('kode_perusahaan')

    filterId = '%'
    filterItemCode = '%'
    try:
        sql = """
        SELECT
                	header.id AS idh,
                	header.item_code,
                	header.stock_code,
                	header.description,
                	header.category_code,
                	cat.description AS category_name,
                	header.quantity,
                	header.unit_code,
                	head_unit.`name` AS unit_name,
                	header.biaya,
                	header.is_combo,
                	header.kode_perusahaan,
                	line.id AS line_id,
                	line.item_code AS line_item_code,
                	line.stock_code AS line_stock_code,
                	line_item.description AS line_item_desc,
                	line.quantity AS line_qty,
                	line.koefisien_harga AS line_koefisien_harga 
                FROM
                	in_01_m_item_codes AS header
                	LEFT JOIN in_04_m_item_combo AS line ON ( line.item_code = header.item_code ) 
                	LEFT JOIN in_02_m_stock_category AS cat ON (cat.category_code=header.category_code)
	                LEFT JOIN in_01_m_item_units AS head_unit ON (head_unit.`code`= header.unit_code)
	                LEFT JOIN in_01_m_item_codes AS line_item ON (line_item.item_code=line.stock_code)
                WHERE
                	header.is_combo = 1 
                	AND header.active = 1 
                	AND header.delete_mark = 0
                    AND header.kode_perusahaan LIKE %s 
                    AND header.id LIKE %s
                    AND header.item_code LIKE %s
          """
        if params is not None:
            if params.get('id'):
                filterId = params.get('id')
            if params.get('item_code'):
                filterItemCode = params.get('item_code')
                #sql += " AND header.id = '" + params.get('id') + "'"
    
        cur.execute(sql, (inKodePerusahaan, filterId, filterItemCode))
        myresult = cur.fetchall()
        returnData = []
        lineItemData = []
        headerData = []
        tempHeader = 0
        for row in myresult:
            idHeader = row['idh']
            if tempHeader!=0 and idHeader!=tempHeader:
                headerData['line_items'] = lineItemData
                lineItemData = []
                returnData.append(headerData)
                
            headerData = {
                "idh": row['idh'],
                "item_code": row['item_code'],
                "stock_code": row['stock_code'],
                "description": row['description'],
                "category_code": row['category_code'],
                "category_name": row['category_name'],
                "quantity": row['quantity'],
                "unit_code": row['unit_code'],
                "unit_name": row['unit_name'],
                
                "biaya": row['biaya'],
                
                "is_combo": row['is_combo'],
                "kode_perusahaan": row['kode_perusahaan']
            }
            linenya = {
                "line_id": row['line_id'],
                "line_item_code": row['line_item_code'],
                "line_stock_code": row['line_stock_code'],
                "line_item_desc": row['line_item_desc'],
                "line_qty": row['line_qty'],
                "line_koefisien_harga": row['line_koefisien_harga']
            }
            lineItemData.append(linenya)
            tempHeader = idHeader
        
        if(len(lineItemData) > 0):
            headerData['line_items'] = lineItemData
        lineItemData = []
        returnData.append(headerData)
    except Exception as e:
        return inc_def.send_response_data(str(e), 500)

    cur.close()
    return inc_def.send_response_data(returnData, 200)

# =============================== FUNCTION POST
def functionPost(con, event):
    con.begin()
    curs = con.cursor()
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    headers = event['headers']
    inKodePerusahaan = headers.get('kode_perusahaan')
    
    field = {
        "item_code": req['item_code'],
    }
    
    isDouble = inc_db.checkDoubleData(curs, field, tableName)
    
    if isDouble == "true" :
        return inc_def.send_response_data("Data is exist", 500)
        
    try:
        if not inKodePerusahaan:
            return inc_def.send_response_data("Company not found!", 404)
        
        stritm = ''
        for row in req['line_items']:
            linenya = req['item_code'] + "=-="
            linenya += row['line_stock_code'] + "=-="
            linenya += row['line_qty'] + "=-="
            linenya += row['line_koefisien_harga'] + "=-="
            linenya += row['line_active'] + "=-="
            linenya += inKodePerusahaan
            if(stritm==''):
                stritm = linenya
            else:
                stritm = stritm + "=+=" + linenya
        
        sqlInsert = """
        call  `in_m_item_combo`
        ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cur.execute(sqlInsert, (idMenu, str(req['item_code']), str(req['stock_code']), str(req['description']), req['category_code'], req['quantity'], req['is_foreign'], str(req['unit_code']), req['berat'], req['panjang'], req['lebar'], req['tinggi'], req['biaya'], req['promo_active'], req['koefisien'], req['is_combo'], str(inKodePerusahaan), req['active'], vToken['id_user'], stritm))
        
        rowTrans = cur.fetchone()
        idh = rowTrans['get_last_id_insert']
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e) , 500)
    else:
        con.commit()
        if(idh>0):
            data = {
                "idh": idh
            }
        else:
            data = {
                "error": "failed"
            }
        cur.close()
        return inc_def.send_response_data(data, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']
    headers = event['headers']
    inKodePerusahaan = headers.get('kode_perusahaan')
    params = event['queryStringParameters']
    id = params['id']

    try:
        sqlUpdate = """
        UPDATE `in_01_m_item_codes` 
        SET
            `item_code` = %s, 
            `stock_code` = %s, 
            `description` = %s, 
            `category_code` = %s, 
            `quantity` = %s, 
            `is_foreign` = %s, 
            `unit_code` = %s, 
            `berat` = %s, 
            `panjang` = %s, 
            `lebar` = %s, 
            `tinggi` = %s, 
            `biaya` = %s, 
            `promo_actived` = %s, 
            `koefisien` = %s, 
            `is_combo` = %s, 
            `kode_perusahaan` = %s, 
            `active` = %s, 
            `update_by` = %s, 
            `update_date` = %s
        WHERE 
            `id` = %s
        """
        cur.execute(sqlUpdate, (
            req['item_code'], 
            req['stock_code'], 
            req['description'], 
            req['category_code'], 
            req['quantity'], 
            req['is_foreign'], 
            req['unit_code'], 
            req['berat'], 
            req['panjang'], 
            req['lebar'], 
            req['tinggi'], 
            req['biaya'], 
            req['promo_active'], 
            req['koefisien'], 
            req['is_combo'], 
            inKodePerusahaan, 
            req['active'], 
            vToken['id_user'], 
            datetime.date.today(), 
            id
        ))
        #inDelete = []
        numberDelete = ''
        for key, rowe in enumerate(req['line_items']):
            linenya = str(rowe['line_id'])
            if (key + 1) == len(req['line_items']) :
                numberDelete += linenya
            else :
                numberDelete += linenya + ", "
            
            # inDelete.append(linenya)
        #notInDelete = inDelete
        #notInDelete = tuple(notInDelete)
        #hapusLine = {'notInDelete': notInDelete}
        sqlDeleteLine = """
                UPDATE `in_04_m_item_combo` 
                SET
                    `active` = %s, 
                    `delete_by` = %s, 
                    `delete_date` = %s,
                    `delete_mark` = 1
                WHERE 
                    `id` NOT IN (%s) AND item_code = %s 
                """
        cur.execute(sqlDeleteLine, (req['active'], vToken['id_user'], datetime.date.today(), numberDelete, req['item_code']))
            
        for row in req['line_items']:
            if (row['line_id'] >0):
                sqlUpdateLine = """
                UPDATE `in_04_m_item_combo` 
                SET
                    `item_code` = %s, 
                    `stock_code` = %s, 
                    `quantity` = %s, 
                    `koefisien_harga` = %s, 
                    `kode_perusahaan` = %s, 
                    `active` = %s, 
                    `update_by` = %s, 
                    `update_date` = %s
                WHERE 
                    `id` = %s
                """
                cur.execute(sqlUpdateLine, (
                    row['line_item_code'], 
                    row['line_stock_code'], 
                    row['line_qty'], 
                    row['line_koefisien_harga'], 
                    inKodePerusahaan, 
                    row['line_active'], 
                    vToken['id_user'], 
                    datetime.date.today(), 
                    row['line_id']
                ))
            else:
                sqlInsertLine = """
                INSERT INTO `in_04_m_item_combo` 
                    ( `item_code`, `stock_code`, `quantity`, `koefisien_harga`, `kode_perusahaan`, `active`, `create_by`, `create_date`, `delete_mark` ) 
                VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, 0)
                """
                cur.execute(sqlInsertLine, (
                    row['line_item_code'], 
                    row['line_stock_code'], 
                    row['line_qty'], 
                    row['line_koefisien_harga'], 
                    inKodePerusahaan, row['line_active'], 
                    vToken['id_user'], 
                    datetime.date.today()
                ))
            
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur.close()
    return inc_def.send_response_data("Updated!", 200)

# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    sqlDelete = """
    UPDATE
    	in_01_m_item_codes AS a
    	JOIN in_04_m_item_combo AS b ON (a.item_code=b.item_code)
    SET
        a.`delete_by` = %s, 
        a.`delete_date` = %s, 
        b.`delete_by` = %s, 
        b.`delete_date` = %s,
        a.`delete_mark` = 1,
        b.`delete_mark` = 1 
    WHERE 
        a.`id` = %s
    """

    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    con.commit()

    cur.close()
    return inc_def.send_response_data("Deleted!", 200)
