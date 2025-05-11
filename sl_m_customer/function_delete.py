import json
import datetime
import pymysql.cursors
import includes_definitions as inc_def
import includes_db_general as inc_db


def delete_customer(con, event):
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    customer = req.get('customer')
    # branch = req.get('branch')
    
    cekSq = issetForeignKeyCustomerWithDeleteMark('sl_01_t_sales_quotation', 'customer_code', customer, con)
    cekSo = issetForeignKeyCustomerWithDeleteMark('sl_01_t_sales_order', 'customer_code', customer, con)
    cekBranch = issetForeignKeyCustomerWithDeleteMark('sl_03_m_customer_branch', 'customer_code', customer, con)
    
    if cekSq > 0 or cekSo > 0 :
        return {
            "code": 500,
            "data": "Customer terdapat dalam transaksi SQ/SO"
        }
    if cekBranch > 0:
        return {
            "code": 500,
            "data": "Customer masih memiliki Branch, hapus branch terlebih dahulu"
        }
    
    try:
        sqlDelete = """
            UPDATE `sl_02_m_customer` 
            SET 
                delete_by = %s, 
                delete_date = %s, 
                delete_mark = 1 
            WHERE 
                customer_code = %s
        """
        cur.execute(sqlDelete, (vToken['id_user'], datetime.datetime.now(tz_JKT), customer))
        
        # sqlDeleteBranch = """
        #     UPDATE `sl_03_m_customer_branch` 
        #     SET 
        #         delete_by = %s, 
        #         delete_date = %s, 
        #         delete_mark = 1 
        #     WHERE 
        #         customer_code = %s
        #         AND branch_code = %s
        # """
        # cur.execute(sqlDeleteBranch, (vToken['id_user'], datetime.datetime.now(tz_JKT), customer, branch))
        
        # sqlDeleteContact = """
        #     UPDATE `sl_04_m_cust_contact_person` 
        #     SET 
        #         delete_by = %s, 
        #         delete_date = %s, 
        #         delete_mark = 1 
        #     WHERE 
        #         customer_code = %s
        #         AND branch_code = %s
        # """
        # cur.execute(sqlDeleteContact, (vToken['id_user'], datetime.datetime.now(tz_JKT), customer, branch))
        
        
        inc_db.addAuditMaster(con, event['id_menu'], inc_def.getActionDelete(), customer, "", event['tableName'], vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return {
            "code": 500,
            "data": str(e)
        }
    else:
        con.commit()
        
    sql = "SELECT customer_code, name, customer_ref FROM `sl_02_m_customer` WHERE customer_code = %s"
    cur.execute(sql, (debtor_no))
    row = cur.fetchone()
    
    cur.close()
    return {
        "code": 200,
        "data": row
    }
    
    
def issetForeignKeyCustomerWithDeleteMark(tableName, field, customer, con):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT COUNT(*) total FROM `" + tableName + "` WHERE `" + field + "` = %s AND delete_mark = 0"
    cur.execute(sql, (customer))
    row = cur.fetchone()
    return int(row['total'])