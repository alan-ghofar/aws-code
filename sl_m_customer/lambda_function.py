import json
import datetime
import pymysql.cursors
import valid_token as v_token
import valid_menu as v_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db
import dateutil.tz
import function_create
import function_delete


# id menu cek table sc_02_menu
tableName = 'sl_02_m_customer'


def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 404)
    
    global idMenu
    global tz_JKT
    
    tz_JKT = dateutil.tz.gettz('Asia/Jakarta')
    
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
    pathMenunya = inc_db.getPathMenu(con, idMenu)
    
    pathFull = pathMenunya + "/" + pathActionnya
    
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
            event['id_menu'] = idMenu
            event['timeZone'] = tz_JKT
            event['tableName'] = tableName
    
    return getHttpMethod(con, httpMethod, event)


def getHttpMethod(con, httpMethod, event):
    if httpMethod == 'GET':
        return functionGet(con, event)
    elif httpMethod == 'POST':
        return functionPost(con, event)
    elif httpMethod == 'PUT':
        return functionPut(con, event)
    elif httpMethod == 'DELETE':
        return functionDelete(con, event)


# =============================== FUNCTION GET
def functionGet(con, event):
    cur = con.cursor(pymysql.cursors.DictCursor)
    params = event['queryStringParameters']

    sql = """
    SELECT 
        cust.customer_code,
        cust.name,
        cust.customer_ref,
        cust.address,
        cust.tax_no,
        cust.curr_code,
        curr.currency_name,
        cust.credit_status_id,
        crs.reason_description credit_status_name,
        cust.payment_terms_indicator,
        pyt.terms payment_terms,
        cust.discount,
        cust.pymt_discount,
        cust.credit_limit,
        cust.notes,
        cust.pemilik,
        cust.barcode,
        cust.promo_actived,
        cust.deposit_account,
        cust.aktivasi_password,
        cust.`password`,
        cust.changed_password,
        cust.date_birth,
        cust.is_pkp,
        cust.last_visited,
        cust.jenis_kirim_code,
        jkf.description jenis_kirim_name,
        cust.active `status`,
        cust.tax_included,
        taxinc.nama_tax_inc
    FROM 
        `sl_02_m_customer` cust
        LEFT JOIN `st_01_tax_included` taxinc ON ( cust.tax_included = taxinc.id )
        LEFT JOIN `st_01_m_currency` curr ON ( curr.currency_id = cust.curr_code )
        LEFT JOIN `sl_01_m_jenis_kirim_faktur` jkf ON ( jkf.code = cust.jenis_kirim_code )
        LEFT JOIN `sl_01_m_credit_status` crs ON ( crs.id = cust.credit_status_id )
        LEFT JOIN `sl_01_m_payment_terms` pyt ON ( pyt.terms_indicator = cust.payment_terms_indicator )
    WHERE 
        cust.delete_mark = 0
        AND cust.active = 1
    """
    
    if params is not None:
        if params.get('id'):
            sql += " AND cust.customer_code = '" + params.get('id') + "'"
        if params.get('customer_name'):
            if (params.get('customer_name') != '*'):
                sql += " AND cust.name LIKE '%" + params.get('customer_name') + "%'"

    cur.execute(sql)
    result = cur.fetchall()

    allData = []
    for row in result:
        data = {
            "customer_code": row['customer_code'],
            "name": row['name'],
            "customer_ref": row['customer_ref'],
            "address": row['address'],
            "tax_no": row['tax_no'],
            "curr_code": row['curr_code'],
            "currency_name": row['currency_name'],
            "credit_status_id": row['credit_status_id'],
            "credit_status_name": row['credit_status_name'],
            "payment_terms_indicator": row['payment_terms_indicator'],
            "payment_terms": row['payment_terms'],
            "discount": row['discount'],
            "pymt_discount": row['pymt_discount'],
            "credit_limit": row['credit_limit'],
            "notes": row['notes'],
            "pemilik": row['pemilik'],
            "barcode": row['barcode'],
            "promo_actived": row['promo_actived'],
            "deposit_account": row['deposit_account'],
            "aktivasi_password": row['aktivasi_password'],
            "password": row['password'],
            "changed_password": row['changed_password'],
            "date_birth": row['date_birth'],
            "is_pkp": row['is_pkp'],
            "last_visited": row['last_visited'],
            "jenis_kirim_code": row['jenis_kirim_code'],
            "jenis_kirim_name": row['jenis_kirim_name'],
            "tax_included": row['tax_included'],
            "nama_tax_inc": row['nama_tax_inc'],
            "status": row['status'],
        }
        allData.append(data)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    data = function_create.insert_customer(con, event)
    
    return inc_def.send_response_data(data['data'], data['code'])


# =============================== FUNCTION POST LAMA
def functionPostLama(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    try:
        sqlInsert = """
        INSERT INTO `sl_02_m_customer`
            (
                name,
                customer_ref,
                address,
                tax_no,
                curr_code,
                tax_included,
                credit_status_id,
                payment_terms_indicator,
                discount,
                pymt_discount,
                credit_limit,
                notes,
                pemilik,
                barcode,
                promo_actived,
                deposit_account,
                aktivasi_password,
                password,
                changed_password,
                date_birth,
                is_pkp,
                last_visited,
                jenis_kirim_code, 
                active, 
                create_by, 
                create_date
            )
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sqlInsert, (req['name'], req['customer_ref'], req['address'], req['tax_no'], 
                req['curr_code'], req['tax_included'], req['credit_status_id'], req['payment_terms_indicator'], 
                req['discount'], req['pymt_discount'], req['credit_limit'], req['notes'], req['pemilik'], 
                req['barcode'], req['promo_actived'], req['deposit_account'], req['aktivasi_password'], 
                req['password'], req['changed_password'], req['date_birth'], req['is_pkp'], req['last_visited'], 
                req['jenis_kirim_code'], req['active'], vToken['id_user'], datetime.datetime.now(tz_JKT)))
        id = con.insert_id()
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    
    # #tambahkan customer branch nya 1
    # customerCode = id
    # branchCode = 1
    # sqlInsertBranch = """
    # 	INSERT INTO sl_03_m_customer_branch 
    # 	(branch_code, customer_code, br_name, branch_ref, br_address, 
        
    # 	contact_name, kode_lokasi, tax_group_id, 
    # 	sales_account, sales_discount_account, receivables_account, payment_discount_account,
    # 	default_ship_via, disable_trans, br_post_address,  notes, 
    # 	nppkp, nama_nppkp, alamat_nppkp, ktp, alamat_ktp, lat, lng,  keakuratan, 
    # 	create_by, create_date, active, delete_mark)
    # 	VALUES
    # 	(%s, %s, %s, %s, %s, 
    # 	%s, %s, %s,
    # 	%s, %s, %s,
    # 	%s, %s, %s, %s,
    # 	%s, %s, %s, %s, 
    # 	%s, %s, %s, %s, %s, %s, %s, %s,
    # 	%s, %s, %s, %s
    # 	)
    # 	"""
    # cur.execute(sqlInsertBranch, (branchCode, customerCode, req['name'], req['customer_ref'], req['address'],
    # 		req['contact_name'], req['kode_lokasi'], req['tax_group_id'],
    # 		req['sales_account'], req['sales_discount_account'], req['receivables_account'], req['payment_discount_account'], 
    # 		req['default_ship_via'], req['disable_trans'], req['br_post_address'],  req['notes'], 
    # 		req['nppkp'], req['nama_nppkp'], req['alamat_nppkp'], req['ktp'], req['alamat_ktp'], req['lat'], req['lng'], req['keakuratan'],
    # 		vToken['id_user'], datetime.datetime.now(tz_JKT), 1, 0))
    # idBranch = con.insert_id()
    # inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), idBranch, '', 'sl_03_m_customer_branch', vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    sql = "SELECT customer_code, name, customer_ref, active FROM `sl_02_m_customer` WHERE customer_code = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "customer_code": row[0],
        "name": row[1],
        "customer_ref": row[2],
    }
    
    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    
    try:
        sqlUpdate = """
        UPDATE `sl_02_m_customer`
        SET
            `name` = %s,
            `customer_ref` = %s,
            `address` = %s,
            `tax_no` = %s,
            `curr_code` = %s,
            `tax_included` = %s,
            `credit_status_id` = %s,
            `payment_terms_indicator` = %s,
            `discount` = %s,
            `pymt_discount` = %s,
            `credit_limit` = %s,
            `notes` = %s,
            `pemilik` = %s,
            `barcode` = %s,
            `promo_actived` = %s,
            `deposit_account` = %s,
            `aktivasi_password` = %s,
            `password` = %s,
            `changed_password` = %s,
            `date_birth` = %s,
            `is_pkp` = %s,
            `last_visited` = %s,
            `jenis_kirim_code` = %s,
            `active` = %s,
            `update_by` = %s,
            `update_date` = %s
        WHERE
            customer_code = %s
        """
        
        cur.execute(sqlUpdate, (req['name'], req['customer_ref'], req['address'], 
                req['tax_no'], req['curr_code'], req['tax_included'], req['credit_status_id'], 
                req['payment_terms_indicator'], req['discount'], req['pymt_discount'], 
                req['credit_limit'], req['notes'], req['pemilik'], req['barcode'], 
                req['promo_actived'], req['deposit_account'], req['aktivasi_password'], 
                req['password'], req['changed_password'], req['date_birth'], req['is_pkp'], 
                req['last_visited'], req['jenis_kirim_code'], req['active'], vToken['id_user'], 
                datetime.datetime.now(tz_JKT), id))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    sql = "SELECT customer_code, name, customer_ref, active FROM `sl_02_m_customer` WHERE customer_code = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "customer_code": row[0],
        "name": row[1],
        "customer_ref": row[2],
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    data = function_delete.delete_customer(con, event)
    
    return inc_def.send_response_data(data['data'], data['code'])


# =============================== FUNCTION DELETE LAMA
def functionDeleteLama(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    customer = params.get('customer')
    branch = params.get('branch')
    
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
        
        sqlDeleteBranch = """
            UPDATE `sl_03_m_customer_branch` 
            SET 
                delete_by = %s, 
                delete_date = %s, 
                delete_mark = 1 
            WHERE 
                customer_code = %s
                AND branch_code = %s
        """
        cur.execute(sqlDeleteBranch, (vToken['id_user'], datetime.datetime.now(tz_JKT), customer, branch))
        
        sqlDeleteContact = """
            UPDATE `sl_04_m_cust_contact_person` 
            SET 
                delete_by = %s, 
                delete_date = %s, 
                delete_mark = 1 
            WHERE 
                customer_code = %s
                AND branch_code = %s
        """
        cur.execute(sqlDeleteContact, (vToken['id_user'], datetime.datetime.now(tz_JKT), customer, branch))
        
        keterangan = "Customer: " + customer + ", branch: " + branch
        
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), customer, keterangan, tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()
    
    data = {
        "message": "Deleted!"
    }
    
    cur.close()
    return inc_def.send_response_data(data, 200)