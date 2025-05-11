import json
import datetime
import pymysql.cursors
import includes_definitions as inc_def
import includes_db_general as inc_db


def insert_so(con, event):
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    idMenu = event['idMenu']
    vToken = event['data_user']
    headers = event['headers']
    inKodePerusahaan = headers.get('kode_perusahaan')
    trans_no = req.get('trans_no') if req.get('trans_no') else 0
    typeDocument = event['typeDocument']
    
    if int(trans_no) > 0 :
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
        req['memo'] = ""
        req['tax_group_id'] = "0"
        req['ov_freight'] = "0"
        req['ov_discount'] = "0"
        req['salesman_id'] = "0"
        req['routes_id'] = "0"
        req['line_items'] = []
    
    branchCode = str(req['branch_code'])
    customerCode = str(req['customer_code'])
    
    noTransDocument = 0
    noReferenceDoc = ""
    is_android_toko = 0
    try:
        if not inKodePerusahaan:
            return {
                "code": 404,
                "message": "Company not found!"
            }
            
        if req['status_code'] != 1:
            issetDocumentWorkflow = inc_db.isUsingApprovalDocument(con, typeDocument)
            if issetDocumentWorkflow == False:
                #jika tidak ada approval maka document status langsung 3
                req['status_code'] = "3"
        
        issetStatusDocument = inc_db.isValidDocumentStatus(con, typeDocument, req['status_code'])
        if issetStatusDocument == False:
            return {
                "code": 404,
                "message": "Document status not found!"
            }
        
        
        sql = "SELECT kode_lokasi FROM sl_03_m_customer_branch WHERE branch_code = %s AND customer_code = %s AND delete_mark = 0 AND active = 1 "
        cur.execute(sql, (branchCode, customerCode))
        row = cur.fetchone()
        getKodeLokasi = row['kode_lokasi']
        
        
        stritm = strCombo = ''
        if req.get('line_items') :
            index = 0
            for row in req['line_items']:
                index = index + 1
                # GET tax_type_item_id
                sql = "SELECT `tax_type_item_id`, `is_combo` FROM `in_01_m_item_codes` WHERE item_code = %s "
                cur.execute(sql, (row.get('item_code')))
                rowItem = cur.fetchone()
                tax_type_item_id = rowItem.get('tax_type_item_id')
                    
                linenya = str(0) + "=-="
                linenya += row.get('item_code') + "=-="
                linenya += row['item_name'] + "=-="
                linenya += str(row['quantity']) + "=-="
                linenya += str(row['unit_price']) + "=-="
                linenya += row['uom_input'] + "=-="
                linenya += str(row['qty_input']) + "=-="
                linenya += str(row['price_input']) + "=-="
                linenya += inKodePerusahaan + "=-="
                linenya += str(tax_type_item_id) + "=-="
                linenya += row['memo_line']
                if(stritm==''):
                    stritm = linenya
                else:
                    stritm = stritm + "=+=" + linenya
                
                if str(rowItem.get('is_combo')) == "1" :
                    sqlCombo = """
                    SELECT
                    	combo.stock_code,
                    	stock.description item_name,
                    	combo.quantity,
                    	combo.koefisien_harga,
                    	stock.unit_code uom_input,
                    	( combo.quantity * combo.koefisien_harga ) total_line 
                    FROM
                    	`in_04_m_item_combo` combo
                    	LEFT JOIN in_03_m_stock_master stock ON ( stock.stock_code = combo.stock_code ) 
                    WHERE
                    	combo.item_code = %s 
                    	AND combo.kode_perusahaan = %s 
                    	AND combo.active = 1 
                    	AND combo.delete_mark = 0
                    """
                    cur.execute(sqlCombo, (row.get('item_code'), inKodePerusahaan))
                    resCombo = cur.fetchall()
                    
                    totalHargaKoef = 0
                    for rowCombo1 in resCombo:
                        totalHargaKoef = totalHargaKoef + rowCombo1['total_line']
                    
                    
                    for rowCombo in resCombo:
                        koefHarga = (float(rowCombo['total_line']) / float(totalHargaKoef)) * float(row['unit_price'])
                        
                        
                        lineCombo = str(0) + "=-="
                        lineCombo += row.get('item_code') + "=-="
                        lineCombo += rowCombo.get('stock_code') + "=-="
                        lineCombo += rowCombo.get('item_name') + "=-="
                        lineCombo += str(rowCombo['quantity'] * row['quantity']) + "=-="
                        lineCombo += str(koefHarga) + "=-="
                        lineCombo += row['uom_input'] + "=-="
                        lineCombo += str(rowCombo['quantity']) + "=-="
                        lineCombo += str(rowCombo['koefisien_harga']) + "=-="
                        lineCombo += inKodePerusahaan + "=-="
                        lineCombo += str(tax_type_item_id) + "=-="
                        lineCombo += row['memo_line']
                        
                        if(strCombo == ''):
                            strCombo = lineCombo
                        else:
                            strCombo = strCombo + "=+=" + lineCombo
                    
                else :
                    lineCombo = str(0) + "=-="
                    lineCombo += row.get('item_code') + "=-="
                    lineCombo += row.get('item_code') + "=-="
                    lineCombo += row['item_name'] + "=-="
                    lineCombo += str(row['quantity']) + "=-="
                    lineCombo += str(row['unit_price']) + "=-="
                    lineCombo += row['uom_input'] + "=-="
                    lineCombo += str(row['qty_input']) + "=-="
                    lineCombo += str(row['price_input']) + "=-="
                    lineCombo += inKodePerusahaan + "=-="
                    lineCombo += str(tax_type_item_id) + "=-="
                    lineCombo += row['memo_line']
                    
                    if(strCombo == ''):
                        strCombo = lineCombo
                    else:
                        strCombo = strCombo + "=+=" + lineCombo
        
        # return {
        #     "code": 404,
        #     "message": strCombo
        # }
        
        # sqldebug = """
        # call `sl_so_create_sales_order`
        # ( 
        # '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', 
        # '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', 
        # '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
        # """.format(idMenu, inKodePerusahaan, req.get('trans_no'), '', typeDocument, customerCode, branchCode, 
        # req['trans_date'], req['expired_date'], str(req['sales_type_id']), str(req['tax_included']), 
        # str(req['payment_terms_id']), str(req['ship_via']), req['deliver_to'], req['delivery_address'], 
        # req['contact_phone'], req['customer_ref'], req['memo'], str(req['tax_group_id']), 
        # str(req['ov_freight']), str(req['ov_discount']), getKodeLokasi, str(req['salesman_id']), 
        # str(req['routes_id']), str(req['status_code']), is_android_toko, req['is_direct'], str(vToken['id_user']), trans_no, stritm, strCombo)
        
        # return {
        #     "code": 404,
        #     "message": sqldebug
        # }
        
        
        sqlInsert = """
            call  `sl_so_create_sales_order`
            ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sqlInsert, (idMenu, inKodePerusahaan, 0, None, typeDocument, customerCode, branchCode, 
        req['trans_date'], req['expired_date'], str(req['sales_type_id']), str(req['tax_included']), 
        str(req['payment_terms_id']), str(req['ship_via']), req['deliver_to'], req['delivery_address'], 
        req['contact_phone'], req['customer_ref'], req['memo'], str(req['tax_group_id']), 
        str(req['ov_freight']), str(req['ov_discount']), getKodeLokasi, str(req['salesman_id']), 
        str(req['routes_id']), str(req['status_code']), is_android_toko, str(req['is_direct']), str(vToken['id_user']), trans_no, stritm, strCombo))
        
        rowTrans = cur.fetchone()
        
        noTransDocument = rowTrans['get_trans_no']
        noReferenceDoc = rowTrans['get_reference']
    except Exception as e:
        con.rollback()
        con.close()
        return {
            "code": 404,
            "message": str(e)
        }
    else:
        con.commit()
        if(noTransDocument>0):
            code = 200
            data = {
                "trans_no": noTransDocument,
                "reference": noReferenceDoc
            }
        else:
            code = 400
            data = {
                "error": "failed"
            }
        cur.close()
        return {
            "code": code,
            "message": data
        }