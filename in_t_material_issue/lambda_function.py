import json
import datetime
import valid_token
import valid_menu
import pymysql.cursors
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


tableName = 'in_01_t_material_issue'


def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 404)
    
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
        return inc_def.send_response_data([], 404)
    
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
                if url_path != pathFull:
                    return inc_def.send_response_data("Path menu action doesn't match", 403)
            event['data_user'] = vToken
    
    return getHttpMethod(con, httpMethod, event)


def getHttpMethod(con, httpMethod, event):
    if httpMethod == 'POST':
        return functionPost(con, event)
    elif httpMethod == 'GET':
        return functionGet(con, event)
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
    
    
    # NOTE
    # req['status_code'] -> 1: saat pencet save; 2: saat pencet save and process
    
    try:
        if not inKodePerusahaan:
            return inc_def.send_response_data("Company not found!", 404)
            
        issetDocumentWorkflow = checkDocumentWorkflow(con, typeDocument)
        if issetDocumentWorkflow == "true":
            req['status_code'] = "3"
            
        issetStatusDocument = checkStatusDocument(con, typeDocument, req['status_code'])
        if issetStatusDocument == "false":
            return inc_def.send_response_data("Document status not found!", 404)
        
        stritm = ''
        for row in req['line_items']:
            linenya = row['item_code'] + "=-="
            linenya += row['item_name'] + "=-="
            linenya += str(row['quantity']) + "=-="
            linenya += str(row['unit_price']) + "=-="
            linenya += str(row['uom']) + "=-="
            linenya += str(req['kode_lokasi']) + "=-="
            linenya += inKodePerusahaan + "=-="
            linenya += row['memo']
            if(stritm==''):
                stritm = linenya
            else:
                stritm = stritm + "=+=" + linenya
        
        
        # sqldebug = """
        # call  `in_t_material_issue`
        # ( '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
        # """.format(idMenu, inKodePerusahaan, typeDocument, str(req['mat_issue_type']), str(req['kode_lokasi']), 
        # req['trans_date'], str(req['memo']), str(req['status_code']), str(vToken['id_user']), stritm)
        # return inc_def.send_response_data(sqldebug , 500)
        
        sqlInsert = """
        call  `in_t_material_issue`
        ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sqlInsert, (idMenu, inKodePerusahaan, typeDocument, str(req['mat_issue_type']), str(req['kode_lokasi']), 
        req['trans_date'], str(req['memo']), str(req['status_code']), str(vToken['id_user']), stritm))
        
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
        	h.mat_issue_type,
        	tipe.mat_issue_type AS mat_issue_type_name,
        	h.coa_adj,
        	coa.account_name,
        	h.kode_lokasi,
        	loc.nama_lokasi,
        	h.trans_date,
        	h.total_value,
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
        	in_01_t_material_issue AS h
        	LEFT JOIN in_02_t_material_issue_detail AS l ON ( l.type_doc = h.type_doc AND l.trans_no = h.trans_no )
        	LEFT JOIN st_01_lokasi AS loc ON ( loc.kode_lokasi = h.kode_lokasi AND loc.delete_mark = 0)
        	LEFT JOIN in_01_m_material_issue_types AS tipe ON ( tipe.id = h.mat_issue_type AND tipe.delete_mark = 0)
        	LEFT JOIN st_02_document_status AS stat ON ( stat.status_code = h.status_code AND stat.type_doc=h.type_doc AND stat.delete_mark = 0) 
        	LEFT JOIN fi_03_m_chart_of_account AS coa ON (coa.account_code=h.coa_adj AND coa.delete_mark = 0)
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
        
        # return inc_def.send_response_data(sql, 200)
        
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
                "mat_issue_type": row['mat_issue_type'],
                "mat_issue_type_name": row['mat_issue_type_name'],
                "coa_adj": row['coa_adj'],
                "account_name": row['account_name'],
                "kode_perusahaan": row['kode_perusahaan'],
                "trans_date": row['trans_date'],
                "total_value": row['total_value'],
                "kode_lokasi": row['kode_lokasi'],
                "nama_lokasi": row['nama_lokasi'],
                "memo": row['memo'],
                "status_code": row['status_code'],
                "status_name": row['status_name'],
                "create_by": row['create_by'],
                "create_date": row['create_date']
            }
            linenya = {
                "line_id": row['line_id'],
                "stock_code": row['stock_code'],
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
    
    
def checkStatusDocument(con, type_doc, status_code):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
    SELECT
    	COUNT( type_doc ) hitung 
    FROM
    	st_02_document_status 
    WHERE
    	delete_mark = 0 
    	AND active = 1
    	AND type_doc = %s
    	AND `status_code` = %s
    """
    cur.execute(sql, (type_doc, status_code))
    row = cur.fetchone()
    
    return "true" if row['hitung'] > 0 else "false"

    
def checkDocumentWorkflow(con, type_doc):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
    SELECT
    	COUNT( type_doc ) hitung 
    FROM
    	st_02_document_workflow 
    WHERE
    	delete_mark = 0 
    	AND active = 1
    	AND type_doc = %s
    """
    cur.execute(sql, (type_doc))
    row = cur.fetchone()
    
    return "true" if row['hitung'] > 0 else "false"
    