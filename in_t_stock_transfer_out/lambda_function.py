import json
import datetime
import valid_token
import valid_menu
import pymysql.cursors
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


tableName = 'in_01_t_location_transfer_out'


def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 203)
    
    global idMenu
    global typeDocument
    
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
        return inc_def.send_response_data(['in falid hhtpmethod'], 404)
    
    idMenu = inc_db.getIdMenuByTableName(con, tableName)
    dataMenu = inc_db.getDataMenu(con, idMenu)
    pathMenunya = dataMenu['path']
    codeDocument = dataMenu['code_doc']
    # return inc_def.send_response_data(codeDocument, 404)
    typeDocument = inc_db.getTypeDocument(con, codeDocument)
    
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
                if  url_path != pathFull:
                    return inc_def.send_response_data("Path menu action doesn't match", 403)
            event['data_user'] = vToken
    
    return getHttpMethod(con, httpMethod, event)


def getHttpMethod(con, httpMethod, event):
    if httpMethod == 'POST':
        return functionPost(con, event)
    elif httpMethod == 'GET':
        return functionGet(con, event)
    elif httpMethod == 'DELETE':
        return functionDelete(con, event)
    elif httpMethod == 'PUT':
        return functionPut(con, event)
    else:
        return inc_def.send_response_data([], 404)


# =============================== FUNCTION POST
def functionPost(con, event):
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    headers = event['headers']
    inKodePerusahaan = headers.get('kode_perusahaan')
    trans_no = req.get('trans_no') if req.get('trans_no') else 0
    
    if trans_no > 0 :
        req['status_transfer'] = "0"
        req['trans_date'] = "0"
        req['from_loc'] = "0"
        req['to_loc'] = "0"
        req['memo'] = "0"
        req['line_items'] = []
        
    # NOTE
    # req['status_code'] -> 1: saat pencet save; 2: saat pencet save and process
    
    try:
        if not inKodePerusahaan:
            return inc_def.send_response_data("Company not found!", 404)
        
        if req['status_code'] != 1:
            issetDocumentWorkflow = inc_db.isUsingApprovalDocument(con, typeDocument)
            if issetDocumentWorkflow == False:
                #jika tidak ada approval maka document status langsung 3
                req['status_code'] = "3"
            
        issetStatusDocument = inc_db.isValidDocumentStatus(con, typeDocument, req['status_code'])
        if issetStatusDocument == False:
            return inc_def.send_response_data("Document status not found!", 404)
            
        # return inc_def.send_response_data(req['line_items'], 404)
        
        stritm = ''
        for row in req['line_items']:
            linenya = row['item_code'] + "=-="
            linenya += row['item_name'] + "=-="
            linenya += str(row['quantity']) + "=-="
            linenya += str(row['unit_price']) + "=-="
            # linenya += str(row['uom']) + "=-="
            linenya += '' + "=-="
            linenya += str(req['from_loc']) + "=-="
            linenya += row['memo']
            if(stritm==''):
                stritm = linenya
            else:
                stritm = stritm + "=+=" + linenya
        
        sqlInsert = """
        call  `in_t_stock_transfer`
        ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sqlInsert, (idMenu, inKodePerusahaan, typeDocument, str(req['from_loc']), str(req['to_loc']), '', 0,
        req['status_transfer'], '', 0, req['trans_date'], str(req['memo']), str(req['status_code']), str(vToken['id_user']), stritm, trans_no))

        rowTrans = cur.fetchone()
        noTransDocument = rowTrans['get_trans_no']
        noReferenceDoc = rowTrans['get_reference']
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e) , 500)
    else:
        con.commit()
        if(noTransDocument>0):
            data = {
                "trans_no": noTransDocument,
                "reference": noReferenceDoc
            }
        else:
            data = {
                "error": "failed"
            }
        cur.close()
        return inc_def.send_response_data(data, 200)


# =============================== FUNCTION GET
def functionGet(con, event):
    cur = con.cursor(pymysql.cursors.DictCursor)
    headers = event['headers']
    kodePerusahaan = headers.get('kode_perusahaan')
    params = event['queryStringParameters']
    
    try:
        if not kodePerusahaan:
            return inc_def.send_response_data("Company not found!", 404)
            
        sql = """
    SELECT
        h.trans_no,
        h.type_doc,
        h.reference,
        h.from_loc,
        h.to_loc,
        loc.nama_lokasi,
        h.trans_date,
        h.kode_perusahaan,
        h.status_code,
        stat.status_name,
        h.memo,
        h.create_by,
        h.create_date,
        l.id AS line_id,
        l.stock_code,
        l.item_name,
        l.quantity,
        l.unit_price,
        l.memo AS line_memo,
        ( l.quantity * l.unit_price ) AS line_total 
    FROM
    	in_01_t_location_transfer AS h
    	LEFT JOIN in_02_t_location_transfer_detail AS l ON ( l.type_doc = h.type_doc AND l.trans_no = h.trans_no )
    	LEFT JOIN st_01_lokasi AS loc ON ( loc.kode_lokasi = h.from_loc AND loc.delete_mark = 0 )
    	LEFT JOIN st_02_document_status AS stat ON ( stat.status_code = h.status_code AND stat.type_doc = h.type_doc AND stat.delete_mark = 0 )
    WHERE
        	h.delete_mark = 0
            AND h.kode_perusahaan = %s
            AND h.type_doc = %s
        """
        
        if params is not None:
            if params.get('trans_no') is not None:
                sql += " AND h.trans_no = " + params.get('trans_no')
            if params.get('start_date') is not None:
                sql += " AND h.trans_date >= '" + params.get('start_date') + "'"
            if params.get('end_date') is not None:
                sql += " AND h.trans_date <= '" + params.get('end_date') + "'"
        sql += " ORDER BY h.trans_no, l.id"
        
        #return inc_def.send_response_data(sql, 200)
        
        cur.execute(sql, (kodePerusahaan, typeDocument))
        myresult = cur.fetchall()
        returnData = []
        lineItemData = []
        headerData = []
        temptrans_no=0
        for row in myresult:
            transno = row['trans_no']
            if temptrans_no!=0 and transno!=temptrans_no:
                headerData['line_items'] = lineItemData
                lineItemData = []
                returnData.append(headerData)
                
            headerData = {
                "trans_no": row['trans_no'],
                "type_doc": row['type_doc'],
                "reference": row['reference'],
                "from_loc": row['from_loc'],
                "to_loc": row['to_loc'],
                "kode_perusahaan": row['kode_perusahaan'],
                "trans_date": row['trans_date'],
                "nama_lokasi": row['nama_lokasi'],
                "memo": row['memo'],
                "status_code": row['status_code'],
                "status_name": row['status_name'],
                "create_by": row['create_by'],
                "create_date": row['create_date']
            }
            linenya = {
                "line_id": row['line_id'],
                "item_code": row['stock_code'],
                "item_name": row['item_name'],
                "quantity": row['quantity'],
                "unit_price": row['unit_price'],
                "line_total": row['line_total'],
                "line_memo": row['line_memo']
            }
            lineItemData.append(linenya)
            temptrans_no = transno
        
        if(len(lineItemData) > 0):
            headerData['line_items'] = lineItemData
        lineItemData = []
        returnData.append(headerData)
    except Exception as e:
        return inc_def.send_response_data(str(e), 500)
    
    cur.close()
    return inc_def.send_response_data(returnData, 200)
    
    
# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor(pymysql.cursors.DictCursor)
    params = event['queryStringParameters']
    vToken = event['data_user']
    headers = event['headers']
    trans_no = params.get('trans_no')
    kodePerusahaan = headers.get('kode_perusahaan')
    
        
    sqlCancel = """
        CALL in_adjustments_cancel(%s, %s, %s, %s, %s, %s, %s )
    """
    cur.execute(sqlCancel, (trans_no, typeDocument, inc_def.getStatusCodeCancel(), vToken['id_user'], idMenu, kodePerusahaan, 'CANCEL'))
    
    rowTrans = cur.fetchone()

    cur.close()
    data = {
        "trans_no": trans_no,
        "message": "Cancel!"
    }
    
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    headers = event['headers']
    inKodePerusahaan = headers.get('kode_perusahaan')
    
    try:
        if not inKodePerusahaan:
            return inc_def.send_response_data("Company not found!", 404)
        
        stritm = ''
        if req['line_items'] is not None :
            for row in req['line_items']:
                linenya = str(row['id']) + "=-="
                linenya += str(row['quantity'])
                if(stritm==''):
                    stritm = linenya
                else:
                    stritm = stritm + "=+=" + linenya
        
        
        sqlUpdate = """
            CALL `in_t_stock_transfer_edit`(%s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sqlUpdate, (req.get('trans_no'), typeDocument, str(vToken['id_user']), idMenu, inKodePerusahaan, 'UPDATE', stritm))
        
        rowTrans = cur.fetchone()
        noTransDocument = rowTrans['in_trans_no']
        noReferenceDoc = rowTrans['get_reference']
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e) , 500)
    else:
        con.commit()
        if(noTransDocument>0):
            data = {
                "trans_no": noTransDocument,
                "reference": noReferenceDoc
            }
        else:
            data = {
                "error": "failed"
            }
        cur.close()
        return inc_def.send_response_data(data, 200)