import json
import datetime
import pymysql.cursors
import valid_token as v_token
import valid_menu as v_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


tableName = 'sl_01_t_sales_order-inquiry'


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
    
    typeDocument = inc_db.getTypeDocument(con, codeDocument)
    
    pathFull = pathMenunya + "/" + pathActionnya
    
    # return inc_def.send_response_data(pathFull + ", " + url_path, 403)
    
    if user_token is None:
        return inc_def.send_response_data("Access Denied", 403)
    else:
        vToken = v_token.validationToken(user_token)
        if vToken == "false":
            return inc_def.send_response_data("Invalid Token", 404)
        else:
            vMenu = v_menu.validationMenu(vToken['id_user'], url_path)
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
    else:
        return inc_def.send_response_data([], 404)


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
            so.trans_no,
            so.type_doc,
            so.customer_code,
            cust.`name` customer_name,
            so.branch_code,
            cbranch.br_name,
            so.reference,
            so.customer_ref,
            so.memo,
        	so.trans_date,
        	so.expired_date,
        	so.sales_type_id,
        	stype.sales_type,
        	so.tax_included,
        	taxin.nama_tax_inc,
        	so.payment_terms_id,
        	pterms.terms,
        	so.ship_via,
        	so.delivery_address,
        	so.contact_phone,
        	so.contact_email,
        	so.deliver_to,
        	so.ov_freight,
        	so.ov_gst_freight,
        	so.ov_amount,
        	so.ov_gst,
        	so.kode_lokasi,
        	lok.nama_lokasi,
        	so.delivery_date,
        	so.tax_group_id,
        	txgroup.`name` tax_group,
        	so.salesman_id,
        	msales.salesman_name,
        	so.routes_id,
        	mroute.description route_desc,
        	so.status_code,
        	dstatus.status_name,
        	so.from_android_toko,
        	so.create_by,
        	so.create_date,
        	so_line.id id_line_detail,
        	so_line.item_code,
        	so_line.item_name,
        	so_line.quantity,
        	so_line.x_quantity,
        	so_line.qty_sent,
        	so_line.unit_price,
        	so_line.unit_ov_price,
        	so_line.unit_gst,
        	so_line.discount_percent,
        	so_line.discount_amount,
        	so_line.qty_input,
        	so_line.price_input,
        	so_line.uom_input,
        	so_line.memo memo_line,
        	so_line.quantity_denied 
        FROM
            sl_01_t_sales_order so
            LEFT JOIN sl_02_t_sales_order_details so_line ON ( so.type_doc = so_line.type_doc AND so.trans_no = so_line.trans_no )
            LEFT JOIN sl_02_m_customer cust ON ( cust.customer_code = so.customer_code )
            LEFT JOIN sl_03_m_customer_branch cbranch ON ( cbranch.branch_code = so.branch_code AND cbranch.customer_code = so.customer_code )
            LEFT JOIN sl_01_m_sales_type stype ON ( stype.id = so.sales_type_id )
            LEFT JOIN st_01_tax_included taxin ON ( taxin.id = so.tax_included )
            LEFT JOIN sl_01_m_payment_terms pterms ON ( pterms.terms_indicator = so.payment_terms_id )
            LEFT JOIN st_01_lokasi lok ON ( lok.kode_lokasi = so.kode_lokasi )
            LEFT JOIN st_01_tax_groups txgroup ON ( txgroup.tax_group_id = so.tax_group_id )
            LEFT JOIN sl_04_m_salesman msales ON ( msales.salesman_id = so.salesman_id )
            LEFT JOIN sl_01_m_routes mroute ON ( mroute.id = so.routes_id )
            LEFT JOIN st_02_document_status dstatus ON ( dstatus.type_doc = so.type_doc AND dstatus.status_code = so.status_code ) 
        WHERE
            so.delete_mark = 0 
            AND so.kode_perusahaan = %s
            AND so.type_doc = %s
        """
        
        if params is not None:
            if params.get('trans_no') is not None:
                sql += " AND so.trans_no = " + params.get('trans_no')
            if params.get('start_date') is not None:
                sql += " AND so.trans_date >= '" + params.get('start_date') + "'"
            if params.get('end_date') is not None:
                sql += " AND so.trans_date <= '" + params.get('end_date') + "'"
        sql += " ORDER BY so.trans_no, so_line.id"
        
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
                # "is_problem": row['is_problem'],
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
                "quantity_denied": row['quantity_denied']
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
    