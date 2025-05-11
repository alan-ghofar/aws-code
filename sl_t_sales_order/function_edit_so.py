import json
import datetime
import pymysql.cursors
import includes_definitions as inc_def
import includes_db_general as inc_db


def edit_so(con, event):
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    
    vToken = event['data_user']
    headers = event['headers']
    typeDocument = event['typeDocument']
    idMenu = event['idMenu']
    
    inKodePerusahaan = headers.get('kode_perusahaan')
    
    try:
        if not inKodePerusahaan:
            return inc_def.send_response_data("Company not found!", 404)
        
        
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
                    
                linenya = str(row['id']) + "=-="
                linenya += str(row['quantity']) + "=-="
                linenya += row['uom_selected'] + "=-="
                linenya += str(row['unit_price']) + "=-="
                linenya += str(row['quantity_input'])
                if(stritm==''):
                    stritm = linenya
                else:
                    stritm = stritm + "=+=" + linenya
                
                if str(rowItem.get('is_combo')) == "1" :
                    sqlCombo = """
                    SELECT
                    	stock.id,
                    	combo.stock_code,
                    	combo.quantity,
                    	combo.koefisien_harga,
                    	( combo.quantity * combo.koefisien_harga ) total_line 
                    FROM
                    	sl_02_t_sales_order_details_stock stock
                    	LEFT JOIN `in_04_m_item_combo` combo ON (combo.stock_code = stock.stock_code)
                    WHERE
                    	combo.item_code = %s
                    	AND combo.kode_perusahaan = %s
                    	AND stock.trans_no = %s
                    	AND stock.type_doc = %s
                    	AND combo.active = 1 
                    	AND combo.delete_mark = 0
                    """
                    cur.execute(sqlCombo, (row.get('item_code'), inKodePerusahaan, req.get('trans_no'), typeDocument))
                    resCombo = cur.fetchall()
                    
                    totalHargaKoef = 0
                    for rowCombo1 in resCombo:
                        totalHargaKoef = totalHargaKoef + rowCombo1['total_line']
                    
                    
                    for rowCombo in resCombo:
                        koefHarga = (float(rowCombo['total_line']) / float(totalHargaKoef)) * float(row['unit_price'])
                        
                        
                        lineCombo = str(rowCombo.get('id')) + "=-="
                        lineCombo += row.get('item_code') + "=-="
                        lineCombo += rowCombo.get('stock_code') + "=-="
                        lineCombo += str(rowCombo['quantity'] * row['quantity']) + "=-="
                        lineCombo += str(koefHarga) + "=-="
                        lineCombo += str(rowCombo['quantity']) + "=-="
                        lineCombo += str(rowCombo['koefisien_harga'])
                        
                        if(strCombo == ''):
                            strCombo = lineCombo
                        else:
                            strCombo = strCombo + "=+=" + lineCombo
                    
                else :
                    sqlNonCombo = """
                    SELECT
                    	stock.id
                    FROM
                    	sl_02_t_sales_order_details_stock stock
                    WHERE
                    	stock.src_id_line = %s
                    	AND stock.kode_perusahaan = %s
                    	AND stock.trans_no = %s
                    	AND stock.type_doc = %s
                    """
                    cur.execute(sqlNonCombo, (row.get('id'), inKodePerusahaan, req.get('trans_no'), typeDocument))
                    resNonCombo = cur.fetchone()
                    
                    lineCombo = str(resNonCombo['id']) + "=-="
                    lineCombo += row.get('item_code') + "=-="
                    lineCombo += row.get('item_code') + "=-="
                    lineCombo += str(row['quantity']) + "=-="
                    lineCombo += str(row['unit_price']) + "=-="
                    lineCombo += str(row['quantity_input']) + "=-="
                    lineCombo += str(row['price_input'])
                    
                    if(strCombo == ''):
                        strCombo = lineCombo
                    else:
                        strCombo = strCombo + "=+=" + lineCombo
        
        # sqldebug = """
        # CALL `sl_so_edit`
        # ( '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
        # """.format(req.get('trans_no'), typeDocument, str(vToken['id_user']), idMenu, inKodePerusahaan, 'UPDATE', stritm, strCombo)
        
        # return inc_def.send_response_data(sqldebug, 404)
        
        
        sqlUpdate = """
            CALL `sl_so_edit`(%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sqlUpdate, (req.get('trans_no'), typeDocument, str(vToken['id_user']), idMenu, inKodePerusahaan, 'UPDATE', stritm, strCombo))
        
        rowTrans = cur.fetchone()
        noTransDocument = rowTrans['in_trans_no']
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