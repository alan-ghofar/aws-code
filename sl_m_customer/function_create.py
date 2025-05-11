import json
import datetime
import pymysql.cursors
import string
import random
import includes_definitions as inc_def
import includes_db_general as inc_db
import includes_db_sales as db_sl


def insert_customer(con, event):
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    
    aktivasi_password = generateAktivasiPassword()
    debtor_no = maxNumberCustomer(con) + 1
    branch_code = 1
    
    depo_account = db_sl.getValueConfigSales('deposit_account', con)
    sales_account = db_sl.getValueConfigSales('sales_account', con)
    sales_discount_account = db_sl.getValueConfigSales('sales_discount_account', con)
    receivables_account = db_sl.getValueConfigSales('receivables_account', con)
    payment_discount_account = db_sl.getValueConfigSales('payment_discount_account', con)
        
    try:
        # INSERT KE TABEL MASTER CUSTOMER
        sqlInsert = """
            INSERT INTO `sl_02_m_customer`
            (
                customer_code, name, customer_ref, address, tax_no, curr_code,
                tax_included, credit_status_id, payment_terms_indicator, discount, pymt_discount, credit_limit,
                notes, pemilik, deposit_account, aktivasi_password, password, date_birth,
                is_pkp, jenis_kirim_code, create_by, create_date, active
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, 1
            )
        """
        cur.execute(
            sqlInsert, (
                debtor_no, req['customer_name'], req['customer_short_name'], req['address'], '', req['customer_currency'],
                1, req['credit_status'], req['payment_terms'], (int(req['discount_percent']) / 100), (int(req['prompt_payment_discount_percent']) / 100), req['credit_limit'],
                req['notes'], req['name_ktp'], depo_account, aktivasi_password, '', req['date_birth'],
                req['is_pkp'], req['jenis_kirim'], vToken['id_user'], datetime.datetime.now(event['timeZone'])
            )
        )
        
        
        # INSERT KE TABEL BRANCH CUSTOMER
        sqlInsertBranch = """
            INSERT INTO `sl_03_m_customer_branch`
            (
                customer_code, branch_code, br_name, branch_ref, br_address,
                salesman_id, contact_name, kode_lokasi, tax_group_id, sales_account,
                sales_discount_account, receivables_account, payment_discount_account, default_ship_via, 
                notes, nppkp, nama_nppkp, alamat_nppkp, routes_id, 
                provinsi_code, kabupaten_code, kecamatan_code, 
                ktp, br_post_address, active, create_by, create_date
            )
            VALUES 
            (
                 %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s,
                 %s, %s, %s, %s,
                 %s, %s, %s, %s, %s,
                 %s, %s, %s,
                 %s, %s, %s, %s, %s
            )
        """
        cur.execute(
            sqlInsertBranch, (
                debtor_no, branch_code, req['customer_name'], req['customer_short_name'], req['address'],
                req['sales_person'], req['name_ktp'], req['inventory_location'], req['tax_group'], sales_account,
                sales_discount_account, receivables_account, payment_discount_account, req['shipping_company'],
                req['notes'], req['no_npwp'], req['name_npwp'], req['address_npwp'], req['routes'],
                req['provinsi'], req['kabupaten'], req['kecamatan'],
                req['no_ktp'], req['address_ktp'], 1, vToken['id_user'], datetime.datetime.now(event['timeZone'])
            )
        )
        
        
        # INSERT KE TABLE CONTACT PERSON CUSTOMER
        sqlInsertCustContactPerson = """
            INSERT INTO `sl_04_m_cust_contact_person`
            (
                customer_code, branch_code, first_name, last_name, phone, 
                email, fax, active, create_by, create_date
            )
            VALUES
            (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """
        cur.execute(
            sqlInsertCustContactPerson, (
                debtor_no, branch_code, req['name_ktp'], '', req['phone'],
                req['email'], req['fax_number'], 1, vToken['id_user'], datetime.datetime.now(event['timeZone'])
            )
        )
        
        
        # INSERT KE TABEL CUST BRANCH SALESMAN
        sqlInsertBranchSalesman = """
            INSERT INTO `sl_05_m_cust_branch_salesman`
            (
                customer_code, branch_code, salesman_id, routes_id, active, create_by, create_date
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s
            )
        """
        cur.execute(
            sqlInsertBranchSalesman, (
                debtor_no, branch_code, req['sales_person'], req['routes'], 1, vToken['id_user'], datetime.datetime.now(event['timeZone'])
            )
        )
        
        inc_db.addAuditMaster(con, event['id_menu'], inc_def.getActionCreate(), debtor_no, '', event['tableName'], vToken['id_user'])
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
    
    
def maxNumberCustomer(con):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
        SELECT MAX(customer_code) as `number` FROM sl_02_m_customer
    """
    cur.execute(sql)
    row = cur.fetchone()
    
    return int(row['number'])
    

def maxNumberBranch(con, customer_code):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
        SELECT MAX(branch_code) as `number` FROM sl_03_m_customer_branch WHERE customer_code = %s
    """
    cur.execute(sql, (customer_code))
    row = cur.fetchone()
    
    return int(row['number'])


def generateAktivasiPassword(chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(6))