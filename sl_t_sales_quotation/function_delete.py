import pymysql.cursors
import includes_definitions as inc_def
import includes_db_general as inc_db
import dateutil.tz
import datetime


def functionCancel(con, event, typeDocument, idMenu):
    cur = con.cursor(pymysql.cursors.DictCursor)
    headers = event['headers']
    params = event['queryStringParameters']
    vToken = event['data_user']
    
    trans_no = params.get('trans_no')
    kodePerusahaan = headers.get('kode_perusahaan')
    
    statusDraft = inc_def.getStatusCodeDraft()
    
    sqlHeader = """
        SELECT status_code FROM `sl_01_t_sales_quotation` WHERE trans_no = %s AND type_doc = %s
    """
    
    cur.execute(sqlHeader, (trans_no, typeDocument))
    rowHead = cur.fetchone()
    
    
    if rowHead['status_code'] != str(statusDraft):
        return {
            "code": 400,
            "message": "This document failed to cancel, status document is not Draft!"
        }
        
    sqlCancel = """
        CALL sl_sq_cancel(%s, %s, %s, %s, %s, %s, %s )
    """
    cur.execute(sqlCancel, (trans_no, typeDocument, inc_def.getStatusCodeCancel(), vToken['id_user'], idMenu, kodePerusahaan, 'CANCEL'))
    
    rowTrans = cur.fetchone()

    cur.close()
    return {
        "code": rowTrans['status'],
        "message": "Canceled!"
    }
    

def functionClose(con, event, typeDocument, idMenu):
    cur = con.cursor(pymysql.cursors.DictCursor)
    headers = event['headers']
    params = event['queryStringParameters']
    vToken = event['data_user']
    
    trans_no = params.get('trans_no')
    kodePerusahaan = headers.get('kode_perusahaan')
    
        
    sqlCancel = """
        CALL sl_sq_cancel(%s, %s, %s, %s, %s, %s, %s )
    """
    cur.execute(sqlCancel, (trans_no, typeDocument, inc_def.getStatusCodeClose(), vToken['id_user'], idMenu, kodePerusahaan, 'CLOSE'))
    
    rowTrans = cur.fetchone()

    cur.close()
    return {
        "code": rowTrans['status'],
        "message": "Closed!"
    }


def functionCancelManual(con, event, typeDocument, idMenu):
    cur = con.cursor(pymysql.cursors.DictCursor)
    tz_JKT = dateutil.tz.gettz('Asia/Jakarta')
    headers = event['headers']
    params = event['queryStringParameters']
    vToken = event['data_user']
    
    trans_no = params.get('trans_no')
    kodePerusahaan = headers.get('kode_perusahaan')
    
    dataMenu = inc_db.getDataMenu(con, idMenu)
    namaMenu = dataMenu['nama_menu']
    
    statusDraft = inc_def.getStatusCodeDraft()
    
    sqlHeader = """
        SELECT status_code FROM `sl_01_t_sales_quotation` WHERE trans_no = %s AND type_doc = %s
    """
    
    cur.execute(sqlHeader, (trans_no, typeDocument))
    rowHead = cur.fetchone()
    
    
    if rowHead['status_code'] != str(statusDraft):
        return {
            "code": 400,
            "message": "This document failed to cancel, status document is not Draft!"
        }
    
    try:
        sqlUpdateHeader = """
            UPDATE `sl_01_t_sales_quotation`
            SET
                status_code = %s,
                delete_mark = %s,
                delete_by = %s,
                delete_date = %s
            WHERE
                trans_no = %s
        """
        cur.execute(sqlUpdateHeader, (inc_def.getStatusCodeCancel(), 1, vToken['id_user'], datetime.datetime.now(tz_JKT), trans_no))
        
        sqlDetail = """
            SELECT id, quantity FROM sl_02_t_sales_quotation_details WHERE trans_no = %s AND type_doc = %s
        """
        cur.execute(sqlDetail, (trans_no, typeDocument))
        rowDetail = cur.fetchall()
        
        for row in rowDetail:
            sqlUpdateDetail = """
                UPDATE `sl_02_t_sales_quotation_details`
                SET
                    quantity = %s,
                    x_quantity = %s
                WHERE
                    id = %s
            """
            cur.execute(sqlUpdateDetail, (0, row['quantity'], row['id']))
        
        sqlInsertAuditTrail = """
            CALL add_audit_trail(%s, %s, %s, %s, %s, %s, %s, %s);
        """
        cur.execute(sqlInsertAuditTrail, (typeDocument, trans_no, vToken['id_user'], idMenu, namaMenu, 'CANCEL', '', kodePerusahaan))
    except Exception as e:
        con.rollback()
        con.close()
        return {
            "code": 400,
            "message": str(e)
        }
    else:
        con.commit()

    cur.close()
    return {
        "code": 200,
        "message": "Canceled!"
    }

