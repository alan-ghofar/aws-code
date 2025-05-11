import json
import datetime
import valid_token
import valid_menu
import pymysql.cursors
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

tableName = 'pr_01_t_purchase_requisition'

def lambda_handler(event, context):
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
    else:
        return inc_def.send_response_data([], 404)


# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    headers = event['headers']
    inKodePerusahaan = headers.get('kode_perusahaan')
    trans_no = req.get('trans_no') if req.get('trans_no') else 0
    
    if trans_no > 0 :
        req['supplier_id'] = "0"
        req['trans_date'] = "0"
        req['supplier_reference'] = "0"
        req['kode_lokasi'] = "0"
        req['delivery_to_address'] = "0"
        req['total'] = "0"
        req['memo'] = "0"
        req['tax_included'] = "0"
        req['tax_group_id'] = "0"
        req['type_beli'] = "0"
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
                
        stritm = ''
        for row in req['line_items']:
            linenya = row['stock_code'] + "=-="
            linenya += row['description'] + "=-="
            linenya += row['delivery_date'] + "=-="
            linenya += str(row['unit_price']) + "=-="
            linenya += row['uom_selected'] + "=-="
            linenya += str(row['qty_input']) + "=-="
            linenya += str(row['price_input']) + "=-="
            linenya += str(row['item_tax_type_id']) + "=-="
            linenya += inKodePerusahaan
            if(stritm==''):
                stritm = linenya
            else:
                stritm = stritm + "=+=" + linenya
        
        # return inc_def.send_response_data( stritm, 200)
        
        # sqldebug = """
        # call  `pr_tpor_create_purchase_requisition`
        # ( '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
        # """.format(idMenu, inKodePerusahaan, typeDocument, str(req['supplier_id']), req['memo'], 
        # req['trans_date'], req['supplier_reference'], str(req['kode_lokasi']), str(req['delivery_to_address']), 
        # str(req['total']), str(req['tax_included']), req['tax_group_id'], req['status_code'], 
        # req['type_beli'], str(vToken['id_user']), stritm, trans_no)
        
        # return inc_def.send_response_data(sqldebug, 200)
    
        sqlInsert = """
        Call `pr_tpor_create_purchase_requisition`
        ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cur.execute(sqlInsert, (str(idMenu), inKodePerusahaan, typeDocument, str(req['supplier_id']), req['memo'], req['trans_date'], req['supplier_reference'], 
        req['kode_lokasi'], req['delivery_to_address'], str(req['total']), str(req['tax_included']), str(req['tax_group_id']), str(req['status_code']), 
        str(req['type_beli']), str(vToken['id_user']), stritm, trans_no))
    
        rowTrans = cur.fetchone()
        noTransDocument = rowTrans['get_trans_no']
        noReferenceDoc = rowTrans['get_reference']
    except Exception as e:
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()
        if(noTransDocument>0):
            data = {
                "trans_no": noTransDocument,    
                "reference": noReferenceDoc
            }
        else:
            data = {
                "report":"Failed"
            }
        cur.close()
        return inc_def.send_response_data(data, 200)

# =============================== FUNCTION GET
def functionGet(con, event):
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)
    params = event['queryStringParameters']

    try:
        sql = """
        SELECT
        	tpor.trans_no,
        	tpor.type_doc,
        	tpor.supplier_id,
        	sup.supplier_name,
        	tpor.trans_date,
        	tpor.reference,
        	mtbs.type_beli_name type_beli,
        	tpor.supplier_reference,
        	tpor.kode_lokasi,
        	tpor.delivery_to_address,
        	cur.currency_name,
        	tpor.total,
        	tpor.status_code,
        	taxi.nama_tax_inc tax_included,
        	tpor.tax_group_id,
        	usr.nama_user requestor,
        	tpor.memo,
        	tpor.create_by,
        	tpor.create_date,
        	tpor_detail.id,
        	tpor_detail.trans_no,
        	tpor_detail.type_doc,
        	tpor_detail.stock_code,
        	msm.description category,
        	cog.description class_of_good,
        	tpor_detail.description,
        	tpor_detail.delivery_date,
        	tpor_detail.quantity_ordered,
        	tpor_detail.quantity_received,
        	tpor_detail.qty_invoiced,
        	tpor_detail.unit_price,
        	msm.unit_code,
        	tpor_detail.uom_selected,
        	tpor_detail.qty_input,
        	tpor_detail.price_input,
        	tpor_detail.item_tax_type_id,
        	tpor_detail.create_by,
        	tpor_detail.create_date 
        FROM
        	pr_01_t_purchase_requisition tpor
        	LEFT JOIN pr_02_t_purchase_requisition_detail tpor_detail ON ( tpor_detail.trans_no = tpor.trans_no AND tpor_detail.type_doc = tpor.type_doc)
        	LEFT JOIN st_01_lokasi lok ON ( lok.kode_lokasi = tpor.kode_lokasi )
        	LEFT JOIN pr_02_m_supplier sup ON ( sup.supplier_id = tpor.supplier_id )
        	LEFT JOIN pr_01_m_type_beli_supplier mtbs ON ( mtbs.type_beli_id = tpor.type_beli )
        	LEFT JOIN st_01_document doc ON ( doc.type_doc = tpor.type_doc AND doc.active = 1 AND doc.delete_mark = 0 )
        	LEFT JOIN in_03_m_stock_master msm ON ( msm.stock_code = tpor_detail.stock_code AND msm.active = 1 AND msm.delete_mark = 0 )
        	LEFT JOIN st_02_document_status dos ON ( dos.type_doc = tpor.type_doc AND dos.status_code = tpor.status_code )
        	LEFT JOIN in_01_m_class_of_good cog ON ( cog.cog_code = msm.cog_code )
        	LEFT JOIN st_01_tax_included taxi ON ( taxi.id = sup.tax_included )
        	LEFT JOIN st_01_m_currency cur ON ( cur.currency_id = sup.currency )
        	LEFT JOIN sc_01_user usr ON ( usr.id = tpor.create_by ) 
        WHERE
        	tpor.delete_mark = 0 
        	AND tpor_detail.delete_mark = 0
            AND tpor.type_doc = 
        """ + typeDocument
        if params is not None:
            if params.get('trans_no') is not None:
                sql += " AND tpor.trans_no = " + params.get('trans_no')
            if params.get('start_date') is not None:
                sql += " AND tpor.trans_date >= '" + params.get('start_date') + "'"
            if params.get('end_date') is not None:
                sql += " AND tpor.trans_date <= '" + params.get('end_date') + "'"
            if params.get('supplier'):
                if (params.get('supplier') != '*'):
                    sql += " AND sup.supplier_name LIKE '%" + params.get('supplier') + "%'" 

        sql += " ORDER BY tpor.trans_no, tpor_detail.id"
        cur.execute(sql)
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
                "supplier_id": row['supplier_id'],
                "supplier_name": row['supplier_name'],
                "type_beli": row['type_beli'],
                "trans_date": row['trans_date'],
                "reference": row['reference'],
                "supplier_reference": row['supplier_reference'],
                "kode_lokasi": row['kode_lokasi'],
                "delivery_to_address": row['delivery_to_address'],
                "currency_name": row['currency_name'],
                "total": row['total'],
                "tax_included": row['tax_included'],
                "tax_group_id": row['tax_group_id'],
                "status_code": row['status_code'],
                "create_by": row['create_by'],
                "create_date": row['create_date']
            }
            linenya = {
                "id": row['id'],
                "trans_no": row['trans_no'],
                "type_doc": row['type_doc'],
                "stock_code": row['stock_code'],
                "category": row['category'],
                "class_of_good": row['class_of_good'],
                "description": row['description'],
                "delivery_date": row['delivery_date'],
                "unit_price": row['unit_price'],
                "unit_code": row['unit_code'],
                "quantity_ordered": row['quantity_ordered'],
                "quantity_received": row['quantity_received'],
                "qty_invoiced": row['qty_invoiced'],
                "uom_selected": row['uom_selected'],
                "qty_input": row['qty_input'],
                "price_input": row['price_input'],
                "item_tax_type_id" : row['item_tax_type_id'],
                "create_by": row['create_by']
            }
            lineItemData.append(linenya)
            temptrans_no = transno

        if(len(lineItemData) >0):
            headerData['line_items'] = lineItemData
        lineItemData = []
        returnData.append(headerData)
    except Exception as e:
        return inc_def.send_response_data(str(e), 500)

    cur.close()
    return inc_def.send_response_data(returnData, 200)
