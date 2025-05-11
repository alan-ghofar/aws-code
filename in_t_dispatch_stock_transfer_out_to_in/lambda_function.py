import json
import datetime
import valid_token
import valid_menu
import pymysql.cursors
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


tableName = 'in_01_t_location_transfer-dispatch'


def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 203)
    
    global idMenu
    global typeDocument
    global typeDocumentTo
    
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
    codeDocumentTo = dataMenu['code_doc_to']
    
    typeDocument = inc_db.getTypeDocument(con, codeDocument)
    typeDocumentTo = inc_db.getTypeDocument(con, codeDocumentTo)
    
    pathFull = pathMenunya + "/" + pathActionnya
    
    if user_token is None:
        return inc_def.send_response_data("Access Denied", 403)
    else:
        vToken = valid_token.validationToken(user_token)
        if vToken == "false":
            return inc_def.send_response_data("Invalid Token", 404)
        else:
            vMenu = valid_menu.validationMenu(vToken['id_user'], url_path)
            # return inc_def.send_response_data(url_path, 403)
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
    trans_no_src = req['trans_no_src']
    type_doc_src = req['type_doc_src']
    trans_date = req['trans_date']
    memo = req['memo']
    
    # NOTE
    # req['status_code'] -> 1: saat pencet save; 2: saat pencet save and process
    
    try:
        if not inKodePerusahaan:
            return inc_def.send_response_data("Company not found!", 404)
            
        # issetStatusDocument = inc_db.isValidDocumentStatus(con, typeDocument, req['status_code'])
        # if issetStatusDocument == False:
        #     return inc_def.send_response_data("Document status not found!", 404)
            
        # return inc_def.send_response_data(req['line_items'], 404)
        sql = """
            SELECT
        	from_loc,
        	to_loc,
        	no_asset,
        	isship,
        	status_code,
        	trans_date,
        	status_transfer,
        	to_cabang,
        	from_cabang
        FROM
        	in_01_t_location_transfer 
        WHERE
        	type_doc = %s
        	AND trans_no = %s 
        	AND delete_mark = 0
        """
        cur.execute(sql, (type_doc_src, trans_no_src))
        row = cur.fetchone()
        getfromloc = row['from_loc']
        gettoloc = row['to_loc']
        getasset = row['no_asset']
        getiship = row['isship']
        getfromcabang =row['from_cabang']
        gettocabang =row['to_cabang']
        
        
        
        sqldetail = """
             SELECT
            	id,
            	stock_code,
            	item_name,
            	quantity,
            	unit_price,
            	item_unit,
            	memo 
            	FROM in_02_t_location_transfer_detail 
            WHERE
            	type_doc = %s 
            	AND trans_no = %s
        """
        
        cur.execute(sqldetail, (type_doc_src, trans_no_src))
        myresult = cur.fetchall()
        stritm = ''
        for row_line in myresult:
            linenya = row_line['stock_code'] + "=-="
            linenya += row_line['item_name'] + "=-="
            linenya += str(row_line['quantity']) + "=-="
            linenya += str(row_line['unit_price']) + "=-="
            linenya += str(row_line['item_unit']) + "=-="
            linenya += str(row_line['memo']) + "=-="
            linenya += str(row_line['id'])
            if(stritm==''):
                stritm = linenya
            else:
                stritm = stritm + "=+=" + linenya
        
        
        # return inc_def.send_response_data(stritm , 500)
        sqlInsert = """
        call  `in_t_stock_transfer`
        ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cur.execute(sqlInsert, (idMenu, inKodePerusahaan, typeDocumentTo, str(getfromloc), str(gettoloc), getasset, getiship,
        1, str(type_doc_src), str(trans_no_src), trans_date, str(memo), str(inc_def.getStatusCodeApprove()), str(vToken['id_user']), stritm, 0))

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
        loc.nama_lokasi AS from_loc_nama,
        h.to_loc,
        to_loc.nama_lokasi AS to_loc_nama,
        h.trans_date,
        h.kode_perusahaan,
        h.status_code,
        stat.status_name,
        h.memo,
        h.create_by,
        h.create_date
    FROM
    	in_01_t_location_transfer AS h
    	LEFT JOIN st_01_lokasi AS loc ON ( loc.kode_lokasi = h.from_loc AND loc.delete_mark = 0 )
    	LEFT JOIN st_01_lokasi AS to_loc ON ( to_loc.kode_lokasi = h.to_loc AND to_loc.delete_mark = 0 )
    	LEFT JOIN st_02_document_status AS stat ON ( stat.status_code = h.status_code AND stat.type_doc = h.type_doc AND stat.delete_mark = 0 )
    WHERE
        	h.delete_mark = 0
            AND h.kode_perusahaan = %s
            AND h.type_doc = %s
            AND h.trans_no LIKE %s
            AND h.trans_date >= %s
            AND h.trans_date <= %s
            AND h.status_code = %s
        """
        
        filterStartDate = '%'
        filterTransEndDate = '%'
        filterTransNo = '%'
        filterReference = '%'
        if params is not None:
            if params.get('trans_no') is not None:
                filterTransNo = params.get('trans_no')
            if params.get('start_date') is not None:
                filterStartDate = params.get('start_date')
            if params.get('end_date') is not None:
                filterTransEndDate = params.get('end_date')
        
        
        # return inc_def.send_response_data(inc_def.getStatusCodeApprove(), 200)
        
        cur.execute(sql, (kodePerusahaan, typeDocument, filterTransNo, filterStartDate, filterTransEndDate, inc_def.getStatusCodeComplete()))
        myresult = cur.fetchall()
        returnData = []
        headerData = []
        for row in myresult:
                
            headerData = {
                "trans_no": row['trans_no'],
                "type_doc": row['type_doc'],
                "reference": row['reference'],
                "from_loc": row['from_loc'],
                "nama_lokasi_from": row['from_loc_nama'],
                "to_loc": row['to_loc'],
                "nama_lokasi_to": row['to_loc_nama'],
                "kode_perusahaan": row['kode_perusahaan'],
                "trans_date": row['trans_date'],
                "memo": row['memo'],
                "status_code": row['status_code'],
                "status_name": row['status_name'],
                "create_by": row['create_by'],
                "create_date": row['create_date']
            }
            
            returnData.append(headerData)
    except Exception as e:
        return inc_def.send_response_data(str(e), 500)
    
    cur.close()
    return inc_def.send_response_data(returnData, 200)
    
