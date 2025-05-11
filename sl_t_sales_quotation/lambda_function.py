import json
import datetime
import valid_token
import valid_menu
import pymysql.cursors
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db
import function_delete as func_del


tableName = 'sl_01_t_sales_quotation'


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
    pathMenunya = dataMenu['path'] #inc_db.getPathMenu(con, idMenu)
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
        # req['customer_code'] = "0"
        # req['branch_code'] = "0"
        req['trans_date'] = "0"
        req['expired_date'] = "0"
        req['sales_type_id'] = "0"
        req['tax_included'] = "0"
        req['payment_terms_id'] = "0"
        req['ship_via'] = "0"
        req['deliver_to'] = "0"
        req['delivery_address'] = "0"
        req['contact_phone'] = "0"
        req['customer_ref'] = "0"
        req['memo'] = "0"
        req['tax_group_id'] = "0"
        req['ov_freight'] = "0"
        req['ov_discount'] = "0"
        req['salesman_id'] = "0"
        req['routes_id'] = "0"
        req['line_items'] = []
    
    branchCode = req['branch_code']
    customerCode = req['customer_code']
    
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
            
        
        # GET KODE LOKASI
        sql = "SELECT kode_lokasi FROM sl_03_m_customer_branch WHERE branch_code = %s AND customer_code = %s AND delete_mark = 0 AND active = 1 "
        cur.execute(sql, (branchCode, customerCode))
        row = cur.fetchone()
        getKodeLokasi = row.get('kode_lokasi')
        
        stritm = ''
        if req.get('line_items') :
            for row in req['line_items']:
                # GET tax_type_item_id
                sql = "SELECT `tax_type_item_id`, `is_combo` FROM `in_01_m_item_codes` WHERE item_code = %s "
                cur.execute(sql, (row.get('item_code')))
                rowItem = cur.fetchone()
                tax_type_item_id = rowItem.get('tax_type_item_id')
                
                
                linenya = row['item_code'] + "=-="
                linenya += row['item_name'] + "=-="
                linenya += str(row['quantity']) + "=-="
                linenya += str(row['unit_price']) + "=-="
                linenya += row['uom_selected'] + "=-="
                linenya += str(row['quantity_input']) + "=-="
                linenya += str(row['price_input']) + "=-="
                linenya += inKodePerusahaan + "=-="
                linenya += str(tax_type_item_id) + "=-="
                linenya += row['memo']
                if(stritm==''):
                    stritm = linenya
                else:
                    stritm = stritm + "=+=" + linenya
        
        # sqldebug = """
        # call  `sl_sq_create_sales_quotation`
        # ( '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
        # """.format(idMenu, inKodePerusahaan, typeDocument, str(req['customer_code']), branchCode, 
        # req['trans_date'], req['expired_date'], str(req['sales_type_id']), str(req['tax_included']), 
        # str(req['payment_terms_id']), str(req['ship_via']), req['deliver_to'], req['delivery_address'], 
        # req['contact_phone'], req['customer_ref'], req['memo'], str(req['tax_group_id']), 
        # str(req['ov_freight']), str(req['ov_discount']), getKodeLokasi, str(req['salesman_id']), 
        # str(req['routes_id']), str(req['status_code']), str(vToken['id_user']), trans_no, stritm)
        
        # return inc_def.send_response_data(sqldebug, 404)
        
        
        sqlInsert = """
        call  `sl_sq_create_sales_quotation`
        ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
          %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sqlInsert, (idMenu, inKodePerusahaan, typeDocument, str(req['customer_code']), branchCode, 
        req['trans_date'], req['expired_date'], str(req['sales_type_id']), str(req['tax_included']), 
        str(req['payment_terms_id']), str(req['ship_via']), req['deliver_to'], req['delivery_address'], 
        req['contact_phone'], req['customer_ref'], req['memo'], str(req['tax_group_id']), 
        str(req['ov_freight']), str(req['ov_discount']), getKodeLokasi, str(req['salesman_id']), 
        str(req['routes_id']), str(req['status_code']), str(vToken['id_user']), trans_no, stritm))
        
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
            sq.trans_no,
            sq.type_doc,
            sq.customer_code,
            cust.`name` customer_name,
            sq.branch_code,
            cbranch.br_name,
            sq.reference,
            sq.customer_ref,
            sq.memo,
            sq.trans_date,
            sq.expired_date,
            sq.sales_type_id,
            stype.sales_type,
            sq.tax_included,
            taxin.nama_tax_inc,
            sq.payment_terms_id,
            pterms.terms,
            sq.ship_via,
            sq.delivery_address,
            sq.contact_phone,
            sq.contact_email,
            sq.deliver_to,
            sq.ov_freight,
            sq.ov_gst_freight,
            sq.ov_amount,
            sq.ov_gst,
            sq.kode_lokasi,
            lok.nama_lokasi,
            sq.delivery_date,
            sq.tax_group_id,
            txgroup.`name` tax_group,
            sq.salesman_id,
            msales.salesman_name,
            sq.routes_id,
            mroute.description route_desc,
            sq.status_code,
            dstatus.status_name,
            sq.is_problem,
            sq.from_android_toko,
            sq.create_by,
            sq.create_date,
            sq_line.id id_line_detail,
            sq_line.item_code,
            sq_line.item_name,
            sq_line.quantity,
            sq_line.x_quantity,
            sq_line.qty_sent,
            sq_line.unit_price,
            sq_line.unit_ov_price,
            sq_line.unit_gst,
            sq_line.discount_percent,
            sq_line.discount_amount,
            sq_line.qty_input,
            sq_line.price_input,
            sq_line.uom_input,
            sq_line.memo memo_line,
            sq_line.quantity_denied,
            sq_line.item_tax_type_id tax_type_item_id
        FROM
            sl_01_t_sales_quotation sq
            LEFT JOIN sl_02_t_sales_quotation_details sq_line ON ( sq.type_doc = sq_line.type_doc AND sq.trans_no = sq_line.trans_no )
            LEFT JOIN sl_02_m_customer cust ON ( cust.customer_code = sq.customer_code )
            LEFT JOIN sl_03_m_customer_branch cbranch ON ( cbranch.branch_code = sq.branch_code AND cbranch.customer_code = sq.customer_code )
            LEFT JOIN sl_01_m_sales_type stype ON ( stype.id = sq.sales_type_id )
            LEFT JOIN st_01_tax_included taxin ON ( taxin.id = sq.tax_included )
            LEFT JOIN sl_01_m_payment_terms pterms ON ( pterms.terms_indicator  = sq.payment_terms_id)
            LEFT JOIN st_01_lokasi lok ON ( lok.kode_lokasi = sq.kode_lokasi )
            LEFT JOIN st_01_tax_groups txgroup ON ( txgroup.tax_group_id = sq.tax_group_id )
            LEFT JOIN sl_04_m_salesman msales ON ( msales.salesman_id = sq.salesman_id )
            LEFT JOIN sl_01_m_routes mroute ON ( mroute.id = sq.routes_id )
            LEFT JOIN st_02_document_status dstatus ON ( dstatus.type_doc = sq.type_doc AND dstatus.status_code = sq.status_code ) 
        WHERE
            sq.delete_mark = 0 
            AND sq.kode_perusahaan = %s
            AND sq.type_doc = %s
        """
        
        if params is not None:
            if params.get('trans_no') is not None:
                sql += " AND sq.trans_no = " + params.get('trans_no')
            if params.get('reference') is not None:
                sql += " AND sq.reference LIKE '%%" + params.get('reference') + "%%'"
            if params.get('start_date') is not None:
                sql += " AND sq.trans_date >= '" + params.get('start_date') + "'"
            if params.get('end_date') is not None:
                sql += " AND sq.trans_date <= '" + params.get('end_date') + "'"
                
        sql += " ORDER BY sq.trans_no, sq_line.id"
        
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
                "customer_code": row['customer_code'],
                "customer_name": row['customer_name'],
                "branch_code": row['branch_code'],
                "br_name": row['br_name'],
                "reference": row['reference'],
                "customer_ref": row['customer_ref'],
                "memo": row['memo'],
                "trans_date": row['trans_date'],
                "expired_date": row['expired_date'],
                "sales_type_id": row['sales_type_id'],
                "sales_type": row['sales_type'],
                # "price_type_id": row['price_type_id'],
                "tax_included": row['tax_included'],
                "nama_tax_inc": row['nama_tax_inc'],
                "payment_terms_id": row['payment_terms_id'],
                "terms": row['terms'],
                "ship_via": row['ship_via'],
                "delivery_address": row['delivery_address'],
                "contact_phone": row['contact_phone'],
                "contact_email": row['contact_email'],
                "deliver_to": row['deliver_to'],
                "ov_freight": row['ov_freight'],
                "ov_gst_freight": row['ov_gst_freight'],
                "ov_amount": row['ov_amount'],
                "ov_gst": row['ov_gst'],
                "kode_lokasi": row['kode_lokasi'],
                "nama_lokasi": row['nama_lokasi'],
                "delivery_date": row['delivery_date'],
                "tax_group_id": row['tax_group_id'],
                "tax_group": row['tax_group'],
                "salesman_id": row['salesman_id'],
                "salesman_name": row['salesman_name'],
                "routes_id": row['routes_id'],
                "route_desc": row['route_desc'],
                "status_code": row['status_code'],
                "status_name": row['status_name'],
                "is_problem": row['is_problem'],
                "from_android_toko": row['from_android_toko'],
                "create_by": row['create_by'],
                "create_date": row['create_date']
            }
            linenya = {
                "id_line_detail": row['id_line_detail'],
                "item_code": row['item_code'],
                "item_name": row['item_name'],
                "quantity": row['quantity'],
                "x_quantity": row['x_quantity'],
                "qty_sent": row['qty_sent'],
                "unit_price": row['unit_price'],
                "unit_ov_price": row['unit_ov_price'],
                "unit_gst": row['unit_gst'],
                "discount_percent": row['discount_percent'],
                "discount_amount": row['discount_amount'],
                "qty_input": row['qty_input'],
                "price_input": row['price_input'],
                "uom_input": row['uom_input'],
                "memo_line": row['memo_line'],
                "quantity_denied": row['quantity_denied'],
                "tax_type_item_id": row['tax_type_item_id'],
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
    return inc_def.send_response_data(returnData if headerData else [], 200)
    

# =============================== FUNCTION DELETE
def functionDelete(con, event):
    params = event['queryStringParameters']
    vToken = event['data_user']
    trans_no = params.get('trans_no')
    
    if params.get('is_cancel') == "1":
        response_function = func_del.functionCancel(con, event, typeDocument, idMenu)
        return inc_def.send_response_data(response_function['message'], response_function['code'])
        
    if params.get('is_close') == "1":
        response_function = func_del.functionClose(con, event, typeDocument, idMenu)
        return inc_def.send_response_data(response_function['message'], response_function['code'])
    
    return inc_def.send_response_data("emoticon senyum", 200)


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
                linenya += str(row['quantity']) + "=-="
                linenya += row['uom_selected'] + "=-="
                linenya += str(row['unit_price']) + "=-="
                linenya += str(row['quantity_input'])
                if(stritm==''):
                    stritm = linenya
                else:
                    stritm = stritm + "=+=" + linenya
        
        # sqldebug = """
        # CALL `sl_sq_edit`
        # ( '{}', '{}', '{}', '{}', '{}', '{}', '{}')
        # """.format(req.get('trans_no'), typeDocument, str(vToken['id_user']), idMenu, inKodePerusahaan, 'UPDATE', stritm)
        
        # return inc_def.send_response_data(sqldebug, 404)
        
        
        sqlUpdate = """
            CALL `sl_sq_edit`(%s, %s, %s, %s, %s, %s, %s)
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