import json
import datetime
import pymysql.cursors
import includes_definitions as inc_def
import includes_db_general as inc_db


linenya = ''


def is_cancel(con, event):
    global linenya
    
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    
    idMenu = event['idMenu']
    vToken = event['data_user']
    headers = event['headers']
    typeDocument = event['typeDocument']
    
    inKodePerusahaan = headers.get('kode_perusahaan')
    trans_no = req.get('trans_no') if req.get('trans_no') else 0
    
    statusDraft = inc_def.getStatusCodeDraft()
    
    sqlHeader = """
        SELECT status_code FROM `sl_01_t_sales_order` WHERE trans_no = %s AND type_doc = %s
    """
    
    cur.execute(sqlHeader, (trans_no, typeDocument))
    rowHead = cur.fetchone()
    
    if rowHead['status_code'] != str(statusDraft):
        return {
            "code": 400,
            "message": "This document failed to cancel, status document is not Draft!"
        }
    
    status_trans = 0
    status_message = "Failed!"
    try:
        stritm = ''
        if req.get('line_items') :
            for row in req['line_items']:
                linenya += str(row.get('src_id_line')) + "=-="
                linenya += str(row.get('quantity'))
                if(stritm==''):
                    stritm = linenya
                else:
                    stritm = stritm + "=+=" + linenya
        
        sqlCancel = """
            CALL sl_so_cancel(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sqlCancel, (trans_no, typeDocument, inc_def.getStatusCodeCancel(), vToken['id_user'], idMenu, inKodePerusahaan, 'CANCEL', stritm, req.get('is_direct'), 1))
        
        rowTrans = cur.fetchone()
        status_trans = rowTrans['status']
        status_message = "Canceled!"
    except Exception as e:
        con.rollback()
        con.close()
        return {
            "code": 404,
            "message": str(e)
        }
    else:
        con.commit()
        
        return {
            "code": status_trans,
            "message": status_message
        }
    
def is_close(con, event):
    global linenya
    
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    
    params = event['queryStringParameters']
    idMenu = event['idMenu']
    vToken = event['data_user']
    headers = event['headers']
    typeDocument = event['typeDocument']
    
    inKodePerusahaan = headers.get('kode_perusahaan')
    trans_no = req.get('trans_no') if req.get('trans_no') else 0
    
    status_trans = 0
    status_message = "Failed!"
    try:
        stritm = ''
        if req.get('line_items') :
            for row in req['line_items']:
                linenya += str(row.get('src_id_line')) + "=-="
                linenya += str(row.get('quantity'))
                if(stritm==''):
                    stritm = linenya
                else:
                    stritm = stritm + "=+=" + linenya
        
        sqlCancel = """
            CALL sl_so_cancel(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sqlCancel, (trans_no, typeDocument, inc_def.getStatusCodeClose(), vToken['id_user'], idMenu, inKodePerusahaan, 'CLOSE', stritm, req.get('is_direct'), 0))
        
        rowTrans = cur.fetchone()
        status_trans = rowTrans['status']
        status_message = "Closed!"
    except Exception as e:
        con.rollback()
        con.close()
        return {
            "code": 404,
            "message": str(e)
        }
    else:
        con.commit()
        
        return {
            "code": status_trans,
            "message": status_message
        }