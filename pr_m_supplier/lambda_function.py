import json
import datetime
import hashlib
import valid_token
import valid_menu
import database_connection as db_con 
import includes_definitions as inc_def 
import includes_db_general as inc_db 

tableName = 'pr_02_m_supplier'

def lambda_handler(event, context):
    global idMenu
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
    # return inc_def.send_response_data(pathFull, 403)

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
    cur = con.cursor()
    params = event['queryStringParameters']

    sql = """
    SELECT 
        sup.supplier_id,
        sup.supplier_name,
        sup.supplier_short_name,
        sup.supplier_gst_address,
        sup.supplier_gst_name,
        sup.supplier_gst_no,
        sup.supplier_mailing_address,
        sup.supplier_physical_address,
        sup.supplier_website,
        sup.jenis_supplier,
        sup.currency,
        sup.tax_group_id,
        sup.bank_name,
        sup.credit_limit,
        sup.terms_indicator,
        sup.tax_included,
        sup.purchase_account,
        sup.payable_account,
        sup.purchase_discount_account,
        sup.notes,
        sup.deposit_account,
        sup.phone_number,
        sup.secondary_phone_number,
        sup.name_person,
        sup.jenis_si_tax,
        sup.type_diskon,
        sup.supplier_email,
        sup.no_fax,
        sup.our_customer_no
    FROM 
        `pr_02_m_supplier` sup
        LEFT JOIN `st_01_m_currency` mc ON ( mc.currency_id = sup.currency )
        LEFT JOIN `st_01_tax_groups` tg ON ( tg.tax_group_id = sup.tax_group_id )
        LEFT JOIN `sl_01_m_payment_terms` mpt ON ( mpt.terms_indicator = sup.terms_indicator )
        LEFT JOIN `st_01_tax_included` tin ON ( tin.id = sup.tax_included )

    WHERE
        sup.delete_mark = 0
        AND sup.active = 1
    """
    if params is not None:
        if params.get('id'):
            sql += " AND sup.supplier_id = '" + params.get('id') + "'"
        if params.get('supplier'):
            if (params.get('supplier') != '*'):
                sql += " AND sup.supplier_name LIKE '%" + params.get('supplier') + "%'"
    
    cur.execute(sql)
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        menu = {
            "supplier_id": row[0],
            "supplier_name": row[1],
            "supplier_short_name": row[2],
            "supplier_gst_address": row[3],
            "supplier_gst_name": row[4],
            "supplier_gst_no": row[5],
            "supplier_mailing_address": row[6],
            "supplier_physical_address": row[7],
            "supplier_website": row[8],
            "jenis_supplier": row[9],
            "currency": row[10],
            "tax_group_id": row[11],
            "bank_name": row[12],
            "credit_limit": row[13],
            "terms_indicator": row[14],
            "tax_included": row[15],
            "purchase_account": row[16],
            "payable_account": row[17],
            "purchase_discount_account": row[18],
            "notes": row[19],
            "deposit_account": row[20],
            "phone_number": row[21],
            "secondary_phone_number": row[22],
            "name_person": row[23],
            "jenis_si_tax": row[24],
            "type_diskon": row[25],
            "supplier_email": row[26],
            "no_fax": row[27],
            "our_customer_no": row[28],
            # "create_by": row[29],
            # "create_date": row[30],
            # "update_by": row[31],
            # "update_date": row[32],
            # "delete_by": row[33],
            # "delete_date": row[34],
            # "active": row[35],
            # "delete_mark": row[36],
        }
        allData.append(menu)

    cur.close()
    return inc_def.send_response_data(allData, 200)

# =============================== FUNCTION POST
def functionPost(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']
    
    field = {
        "supplier_name": req['supplier_name'],
        "supplier_physical_address": req['supplier_physical_address']
    }
    
    isDouble = inc_db.checkDoubleData(cur, field, tableName)
    
    if isDouble == "true" :
        return inc_def.send_response_data("Data is exist", 400)
        
    try:
        
        purchase = req['purchase_account']
        payable = req['payable_account']
        diskon = req['purchase_discount_account']
        deposit = req['deposit_account']
        
        sql = "SELECT purchase_account, payable_account, purchase_diskon, deposit_account FROM pr_04_m_chart_suppl_type WHERE supplier_type_id = %s"
        
        cur.execute(sql, req['jenis_supplier'])
        row = cur.fetchone()
        if purchase is None:
            purchase = row['purchase_account']
        if payable is None:
            payable = row['payable_account']
        if diskon is None:
            diskon = row['purchase_diskon']
        if deposit is None:
            deposit = row['deposit_account']
        
        sqlInsert = """INSERT INTO `pr_02_m_supplier` (  supplier_name, supplier_short_name, supplier_gst_address, supplier_gst_name, 
        supplier_gst_no, supplier_mailing_address, supplier_physical_address, supplier_website, jenis_supplier, currency, tax_group_id, 
        bank_name, credit_limit, terms_indicator, tax_included, purchase_account, payable_account, purchase_discount_account, notes, deposit_account, 
        phone_number, secondary_phone_number, name_person, jenis_si_tax, type_diskon, supplier_email, no_fax, our_customer_no, create_by, create_date, active, delete_mark) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, 0)"""
        cur.execute(sqlInsert, (req['supplier_name'], req['supplier_short_name'], req['supplier_gst_address'], 
        req['supplier_gst_name'], req['supplier_gst_no'], req['supplier_mailing_address'], req['supplier_physical_address'], req['supplier_website'], 
        req['jenis_supplier'], req['currency'], req['tax_group_id'], req['bank_name'], req['credit_limit'], req['terms_indicator'], 
        req['tax_included'], purchase, payable, diskon, req['notes'], deposit, 
        req['phone_number'], req['secondary_phone_number'], req['name_person'], req['jenis_si_tax'], req['type_diskon'], req['supplier_email'], 
        req['no_fax'], req['our_customer_no'], vToken['id_user'], datetime.date.today()))
        id = con.insert_id()
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()
        
    sql = "SELECT * FROM `pr_02_m_supplier` WHERE supplier_id = %s"
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "supplier_id": row[0],
        "supplier_name": row[1],
        "supplier_short_name": row[2],
        "supplier_gst_address": row[3],
        "supplier_gst_name": row[4],
        "supplier_gst_no": row[5],
        "supplier_mailing_address": row[6],
        "supplier_physical_address": row[7],
        "supplier_website": row[8],
        "jenis_supplier": row[9],
        "currency": row[10],
        "tax_group_id": row[11],
        "bank_name": row[12],
        "credit_limit": row[13],
        "terms_indicator": row[14],
        "tax_included": row[15],
        "purchase_account": row[16],
        "payable_account": row[17],
        "purchase_discount_account": row[18],
        "notes": row[19],
        "deposit_account": row[20],
        "phone_number": row[21],
        "secondary_phone_number": row[22],
        "name_person": row[23],
        "jenis_si_tax": row[24],
        "type_diskon": row[25],
        "supplier_email": row[26],
        "no_fax": row[27],
        "our_customer_no": row[28]
    }

    cur.close()
    return inc_def.send_response_data(data, 200)

# =============================== FUNCTION PUT
def functionPut(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']
    
    try:
    
        sqlUpdate = """
        UPDATE `pr_02_m_supplier` SET supplier_name=%s, supplier_short_name=%s, supplier_gst_address=%s, supplier_gst_name=%s,
        supplier_gst_no=%s, supplier_mailing_address=%s, supplier_physical_address=%s, supplier_website=%s, jenis_supplier=%s, currency=%s,
        tax_group_id=%s, bank_name=%s, credit_limit=%s, terms_indicator=%s, tax_included=%s, purchase_account=%s, payable_account=%s, purchase_discount_account=%s,
        notes=%s, deposit_account=%s, phone_number=%s, secondary_phone_number=%s, name_person=%s, jenis_si_tax=%s, type_diskon=%s, supplier_email=%s,
        no_fax=%s, our_customer_no=%s, update_by=%s, update_date=%s, active=%s WHERE supplier_id=%s
        """
        cur.execute(sqlUpdate, (req['supplier_name'], req['supplier_short_name'], req['supplier_gst_address'], req['supplier_gst_name'], req['supplier_gst_no'], req['supplier_mailing_address'], req['supplier_physical_address'], req['supplier_website'], req['jenis_supplier'],
                req['currency'], req['tax_group_id'], req['bank_name'], req['credit_limit'], req['terms_indicator'], req['tax_included'], req['purchase_account'], req['payable_account'], req['purchase_discount_account'], req['notes'], req['deposit_account'], req['phone_number'],
                req['secondary_phone_number'], req['name_person'], req['jenis_si_tax'], req['type_diskon'], req['supplier_email'], req['no_fax'], req['our_customer_no'], vToken['id_user'], datetime.date.today(), req['active'], id))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()
        

    sql = "SELECT * FROM `pr_02_m_supplier` WHERE supplier_id=%s"

    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "supplier_id": row[0],
        "supplier_name": row[1],
        "supplier_short_name": row[2],
        "supplier_gst_address": row[3],
        "supplier_gst_name": row[4],
        "supplier_gst_no": row[5],
        "supplier_mailing_address": row[6],
        "supplier_physical_address": row[7],
        "supplier_website": row[8],
        "jenis_supplier": row[9],
        "currency": row[10],
        "tax_group_id": row[11],
        "bank_name": row[12],
        "credit_limit": row[13],
        "terms_indicator": row[14],
        "tax_included": row[15],
        "purchase_account": row[16],
        "payable_account": row[17],
        "purchase_discount_account": row[18],
        "notes": row[19],
        "deposit_account": row[20],
        "phone_number": row[21],
        "secondary_phone_number": row[22],
        "name_person": row[23],
        "jenis_si_tax": row[24],
        "type_diskon": row[25],
        "supplier_email": row[26],
        "no_fax": row[27],
        "our_customer_no": row[28],
        "active": row[35]
    }
    cur.close()
    return inc_def.send_response_data(data, 200)
    
# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    #req = json.loads(event['body'])
    params = event['queryStringParameters']
    vToken = event['data_user']
    id = params['id']

    sqlUpdate = """
    UPDATE `pr_02_m_supplier` SET delete_by=%s, delete_date=%s, delete_mark=%s WHERE supplier_id=%s
    """
    cur.execute(sqlUpdate, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    data = {
        "message": "Deleted!"
    }
    con.commit()
    cur.close()
    return inc_def.send_response_data(data, 200)
