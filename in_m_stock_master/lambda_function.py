import json
import pymysql.cursors
import datetime
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

tableName = 'in_03_m_stock_master'

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
    params = event['queryStringParameters']
    
    if (params is not None and params.get('sales_item_kit')=="1"):
        #ambil item_code
        # return inc_def.send_response_data(params.get('sales_item_kit'), 200)
        return getMasterItemCodes(con, event)
    else:
        return getMasterStockItem(con, event)

def getMasterStockItem(con, event):
    #return inc_def.send_response_data(event, 200)
    cur = con.cursor(pymysql.cursors.DictCursor)
    params = event['queryStringParameters']
    sql = """
    	SELECT
    		sm.stock_code,
    		sm.description stock_name,
            sm.long_description,
            sm.category_code,
            sc.description sc_name,
            sm.cog_code,
    		cog.description cog_name,
            sm.unit_code,
            iu.name unit_name,
            sm.mb_flag,
            sm.tax_type_item_id,
            ttitm.`name` tax_type_name,
            sm.is_sale,
            sm.editable,
            sm.default_supplier_id,
            sm.no_sale_android_toko,
            ic.is_foreign,
            ic.promo_actived,
            sm.sales_account,
            sm.cogs_account,
            sm.inventory_account,
            sm.adjustment_account,
            sm.assembly_account,
            sm.barang_hilang_account,
            sm.bonus_penjualan_account,
            sm.retur_cogs_account,
            sm.retur_sales_account,
            sm.potongan_account,
            sm.hutang_sementara_account,
            sm.payable_account,
            sm.receivable_account,
            coa_sales.account_name AS sales_account_name,
            coa_cogs.account_name AS cogs_account_name,
            coa_inventory.account_name AS inventory_account_name,
            coa_adj.account_name AS adjustment_account_name,
            coa_ass.account_name AS assembly_account_name,
            coa_brghilang.account_name AS barang_hilang_account_name,
            coa_bonus.account_name AS bonus_penjualan_account_name,
            coa_retur_cogs.account_name AS retur_cogs_account_name,
            coa_retur_sales.account_name AS retur_sales_account_name,
            coa_potong.account_name AS potongan_account_name,
            coa_hutang_s.account_name AS hutang_sementara_account_name,
            coa_pay.account_name AS payable_account_name,
            coa_receive.account_name AS receivable_account_name,
            sm.active
    	FROM
    		`in_03_m_stock_master` sm
            LEFT JOIN `in_02_m_stock_category` sc ON (sc.category_code = sm.category_code)
    		LEFT JOIN `in_01_m_class_of_good` cog ON (cog.cog_code = sm.cog_code)
            LEFT JOIN `st_01_item_tax_type` ttitm ON (ttitm.tax_type_item_id = sm.tax_type_item_id)
            LEFT JOIN `in_01_m_item_units` iu ON (iu.code = sm.unit_code)
            LEFT JOIN `in_01_m_item_codes` ic ON (ic.stock_code=sm.stock_code)
            LEFT JOIN `fi_03_m_chart_of_account` coa_sales ON (coa_sales.account_code = sm.sales_account)
            LEFT JOIN `fi_03_m_chart_of_account` coa_cogs ON (coa_cogs.account_code = sm.cogs_account)
            LEFT JOIN `fi_03_m_chart_of_account` coa_inventory ON (coa_inventory.account_code = sm.inventory_account)
            LEFT JOIN `fi_03_m_chart_of_account` coa_adj ON (coa_adj.account_code = sm.adjustment_account)
            LEFT JOIN `fi_03_m_chart_of_account` coa_ass ON (coa_ass.account_code = sm.assembly_account)
            LEFT JOIN `fi_03_m_chart_of_account` coa_brghilang ON (coa_brghilang.account_code = sm.barang_hilang_account)
            LEFT JOIN `fi_03_m_chart_of_account` coa_bonus ON (coa_bonus.account_code = sm.bonus_penjualan_account)
            LEFT JOIN `fi_03_m_chart_of_account` coa_retur_cogs ON (coa_retur_cogs.account_code = sm.retur_cogs_account)
            LEFT JOIN `fi_03_m_chart_of_account` coa_retur_sales ON (coa_retur_sales.account_code = sm.retur_sales_account)
            LEFT JOIN `fi_03_m_chart_of_account` coa_potong ON (coa_potong.account_code = sm.potongan_account)
            LEFT JOIN `fi_03_m_chart_of_account` coa_hutang_s ON (coa_hutang_s.account_code = sm.hutang_sementara_account)
            LEFT JOIN `fi_03_m_chart_of_account` coa_pay ON (coa_pay.account_code = sm.payable_account)
            LEFT JOIN `fi_03_m_chart_of_account` coa_receive ON (coa_receive.account_code = sm.receivable_account)
        WHERE
    		sm.`delete_mark` = 0 
    		AND sm.`active` LIKE %s
    		AND sm.stock_code LIKE %s
    		AND sm.is_sale LIKE %s
    		AND (sm.stock_code LIKE %s OR sm.description LIKE %s OR sc.description LIKE %s ) 
        """
    filterId = '%'
    filterIsSale = '%'
    filterQ1 = '%'
    filterActive = '1'
    
    if params is not None:
        if params.get('id'):
            filterId = params.get('id')
        if params.get('is_sale'):
            filterIsSale = params.get('is_sale')
        if params.get('q') and params.get('q') != '*':
            filterQ1 = "%"+params.get('q')+"%"
        if params.get('show_inactive')=="true":
            filterActive = '%'
    
    cur.execute(sql, (filterActive, filterId, filterIsSale, filterQ1, filterQ1, filterQ1))
    myresult = cur.fetchall()
    allData = []
    for row in myresult:
        data = {
            "stock_code" : row['stock_code'],
            "stock_name" : row['stock_name'],
            "long_description" : row['long_description'],
            "category_code" : row['category_code'],
            "sc_name" : row['sc_name'],
            "cog_code" : row['cog_code'],
            "cog_name" : row['cog_name'],
            "tax_type_item_id" : row['tax_type_item_id'],
            "tt_name" : row['tax_type_name'],
            "mb_flag" : row['mb_flag'],
            "is_sale" : row['is_sale'],
            "editable" : row['editable'],
            "default_supplier_id" : row['default_supplier_id'],
            "no_sale_android_toko" : row['no_sale_android_toko'],
            
            "item_code": row['stock_code'],
            "item_name": row['stock_name'],
            "unit_code" : row['unit_code'],
            "unit_name" : row['unit_name'],
            "is_foreign" : row['is_foreign'],
            "promo_actived" : row['promo_actived'],
            
            "sales_account" : row['sales_account'],
            "cogs_account" : row['cogs_account'],
            "inventory_account" : row['inventory_account'],
            "adjustment_account" : row['adjustment_account'],
            "assembly_account" : row['assembly_account'],
            "barang_hilang_account" : row['barang_hilang_account'],
            "bonus_penjualan_account" : row['bonus_penjualan_account'],
            "retur_cogs_account" : row['retur_cogs_account'],
            "retur_sales_account" : row['retur_sales_account'],
            "potongan_account" : row['potongan_account'],
            "hutang_sementara_account" : row['hutang_sementara_account'],
            "payable_account" : row['payable_account'],
            "receivable_account" : row['receivable_account'],
            
            "sales_account_name": row['sales_account_name'],
            "cogs_account_name": row['cogs_account_name'],
            "inventory_account_name": row['inventory_account_name'],
            "adjustment_account_name": row['adjustment_account_name'],
            "assembly_account_name": row['assembly_account_name'],
            "barang_hilang_account_name": row['barang_hilang_account_name'],
            "bonus_penjualan_account_name": row['bonus_penjualan_account_name'],
            "retur_cogs_account_name": row['retur_cogs_account_name'],
            "retur_sales_account_name": row['retur_sales_account_name'],
            "potongan_account_name": row['potongan_account_name'],
            "hutang_sementara_account_name": row['hutang_sementara_account_name'],
            "payable_account_name": row['payable_account_name'],
            "receivable_account_name": row['receivable_account_name'],
            "active" : row['active']
        }
        allData.append(data)
    
    cur.close()
    return inc_def.send_response_data(allData, 200)

def getMasterItemCodes(con, event):
    cur = con.cursor(pymysql.cursors.DictCursor)
    params = event['queryStringParameters']
    sql = """
    	SELECT
        	itm.id,
        	itm.item_code,
        	itm.stock_code,
        	itm.description item_name,
        	itm.category_code,
        	stcat.description category_desc,
        	itm.unit_code,
        	iu.`name` unit_name,
        	itm.is_combo,
        	stk.tax_type_item_id,
        	IFNULL(stk.editable,0) editable,
        	itm.active
        FROM
        	in_01_m_item_codes itm
        	LEFT JOIN in_03_m_stock_master stk 
        	ON ( itm.stock_code = stk.stock_code AND stk.delete_mark = 0 ) 
        	LEFT JOIN in_02_m_stock_category stcat 
        	ON (stcat.category_code = itm.category_code)
        	LEFT JOIN `in_01_m_item_units` iu ON (iu.`code` = itm.unit_code)
        WHERE
        	itm.active LIKE %s
        	AND itm.delete_mark = 0
        	AND itm.item_code LIKE %s
    		AND itm.is_combo LIKE %s
    		AND (itm.item_code LIKE %s OR itm.description LIKE %s OR stcat.description LIKE %s ) 
        """
    
    filterId = '%'
    filterIsCombo = '%'
    filterQ1 = '%'
    filterActive = '1'
    if params is not None:
        if params.get('id'):
            filterId = params.get('id')
        if params.get('is_combo'):
            filterIsCombo = params.get('is_combo')
        if params.get('q') and params.get('q') != '*':
            filterQ1 = "%"+params.get('q')+"%"
        if params.get('show_inactive')=="true":
            filterActive = '%'
    
    cur.execute(sql, (filterActive, filterId, filterIsCombo, filterQ1, filterQ1, filterQ1))
    myresult = cur.fetchall()
    
    allData = []
    for row in myresult:
        data = {
            "id" : row['id'],
            "item_code" : row['item_code'],
            "stock_code" : row['stock_code'],
            "item_name" : row['item_name'],
            "tax_type_item_id": row['tax_type_item_id'],
            "category_code" : row['category_code'],
            "category_desc" : row['category_desc'],
            "unit_code" : row['unit_code'],
            "unit_name" : row['unit_name'],
            "is_combo" : row['is_combo'],
            "editable" : row['editable'],
            "active" : row['active']
        }
        allData.append(data)
    
    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    #return inc_def.send_response_data(event, 400)
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    id = req['stock_code']
    
    sqlInsert_stockMaster = """
        INSERT INTO `in_03_m_stock_master` 
            (
                stock_code,
                category_code,
                tax_type_item_id,
                description,
                long_description,
                unit_code,
                mb_flag,
                is_sale,
                editable,
                default_supplier_id,
                cog_code,
                no_sale_android_toko, 
                
                berat,
                panjang,
                lebar,
                tinggi,
                koefisien,
                
                sales_account,
                cogs_account,
                inventory_account,
                adjustment_account,
                assembly_account,
                barang_hilang_account,
                bonus_penjualan_account,
                retur_cogs_account,
                retur_sales_account,
                potongan_account,
                hutang_sementara_account,
                payable_account,
                receivable_account,
                
                active, 
                create_by, 
                create_date, 
                delete_mark
            ) 
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
        """
    cur.execute(sqlInsert_stockMaster, (
            req['stock_code'],
            req['category_code'],
            req['tax_type_item_id'],
            req['description'],
            req['long_description'],
            req['unit_code'],
            req['mb_flag'],
            req['is_sale'],
            req['editable'],
            req['default_supplier_id'],
            req['cog_code'],
            req['no_sale_android_toko'],
            
            req['berat'],
            req['panjang'],
            req['lebar'],
            req['tinggi'],
            req['koefisien'],
            
            req['sales_account'],
            req['cogs_account'],
            req['inventory_account'],
            req['adjustment_account'],
            req['assembly_account'],
            req['barang_hilang_account'],
            req['bonus_penjualan_account'],
            req['retur_cogs_account'],
            req['retur_sales_account'],
            req['potongan_account'],
            req['hutang_sementara_account'],
            req['payable_account'],
            req['receivable_account'],
            
            req['active'], 
            vToken['id_user'], 
            datetime.date.today()
        ))
    
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])

    if (req['is_sale'] == 1):
        sqlInsert_itemCode = """
        INSERT INTO `in_01_m_item_codes` 
            (
                item_code,
                stock_code,
                description,
                category_code,
                unit_code,
                is_foreign,
                promo_actived,
                active, 
                create_by, 
                create_date, 
                delete_mark
            ) 
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
        """
        cur.execute(sqlInsert_itemCode, (
            id, 
            id, 
            req['description'], 
            req['category_code'],
            req['unit_code'],
            req['is_foreign'],
            req['promo_actived'], 
            req['active'], 
            vToken['id_user'], 
            datetime.date.today()
        ))
        idItemCode = con.insert_id()
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), idItemCode, '', 'in_01_m_item_codes', vToken['id_user'])
    
    con.commit()
    
    sql = """
	SELECT
		sm.stock_code,
        sm.category_code,
        sm.tax_type_item_id,
		sm.description stock_name,
        sm.long_description,
        sm.unit_code,
        sm.mb_flag,
        sm.sales_account,
        sm.cogs_account,
        sm.inventory_account,
        sm.adjustment_account,
        sm.assembly_account,
        sm.is_sale,
        sm.editable,
        sm.default_supplier_id,
        sm.barang_hilang_account,
        sm.bonus_penjualan_account,
        sm.retur_cogs_account,
        sm.retur_sales_account,
		sm.cog_code,
        sm.potongan_account,
        sm.hutang_sementara_account,
        sm.payable_account,
        sm.receivable_account,
        sm.no_sale_android_toko,
        sc.description sc_name,
		cog.description cog_name,
        ttitm.name tt_name,
        iu.name iu_name
	FROM
		`in_03_m_stock_master` sm
        LEFT JOIN `in_02_m_stock_category` sc ON (sc.category_code = sm.category_code)
		LEFT JOIN `in_01_m_class_of_good` cog ON (cog.cog_code = sm.cog_code)
        LEFT JOIN `st_01_item_tax_type` ttitm ON (ttitm.tax_type_item_id = sm.tax_type_item_id)
        LEFT JOIN `in_01_m_item_units` iu ON (iu.code = sm.unit_code)
	WHERE
		sm.`delete_mark` = 0 
		AND sm.`active` = 1
		AND sm.`stock_code` = %s
    """
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "stock_code" : row['stock_code'],
        "category_code" : row['category_code'],
        "tax_type_item_id" : row['tax_type_item_id'],
        "stock_name" : row['stock_name'],
        "long_description" : row['long_description'],
        "unit_code" : row['unit_code'],
        "mb_flag" : row['mb_flag'],
        "is_sale" : row['is_sale'],
        "editable" : row['editable'],
        "default_supplier_id" : row['default_supplier_id'],
        "cog_code" : row['cog_code'],
        "no_sale_android_toko" : row['no_sale_android_toko'],
        "sc_name" : row['sc_name'],
        "cog_name" : row['cog_name'],
        "tt_name" : row['tt_name'],
        "iu_name" : row['iu_name'],
        "sales_account" : row['sales_account'],
        "cogs_account" : row['cogs_account'],
        "inventory_account" : row['inventory_account'],
        "adjustment_account" : row['adjustment_account'],
        "assembly_account" : row['assembly_account'],
        "barang_hilang_account" : row['barang_hilang_account'],
        "bonus_penjualan_account" : row['bonus_penjualan_account'],
        "retur_cogs_account" : row['retur_cogs_account'],
        "retur_sales_account" : row['retur_sales_account'],
        "potongan_account" : row['potongan_account'],
        "hutang_sementara_account" : row['hutang_sementara_account'],
        "payable_account" : row['payable_account'],
        "receivable_account" : row['receivable_account'],
    }
    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION PUT
def functionPut(con, event):
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    
    #get is_sale
    sqlGetSale = """    
    SELECT 
        is_sale
    FROM 
        `in_03_m_stock_master` 
    WHERE 
        stock_code = %s;
    """
    cur.execute(sqlGetSale, (id))
    rowItemSale = cur.fetchone()
    isSaleItem = rowItemSale['is_sale']
    
    
    sqlUpdateStockMaster = """
    UPDATE `in_03_m_stock_master` 
    SET 
        `description` = %s, 
        `long_description` = %s, 
    	`editable` = %s,
    	`default_supplier_id` = %s,
    	`no_sale_android_toko` = %s,
    	`active` = %s,
        `update_by` = %s, 
        `update_date` = %s
    WHERE 
       `stock_code` = %s
    """
    cur.execute(sqlUpdateStockMaster, (req['description'], req['long_description'], 
        req['editable'], req['default_supplier_id'], req['no_sale_android_toko'], req['active'], 
        vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    
    if(isSaleItem==1):
        sqlGetItemCode = """    
        SELECT 
            item_code,
            id
        FROM 
            `in_01_m_item_codes` 
        WHERE 
            stock_code = %s;
        """
        cur.execute(sqlGetItemCode, (id))
        rowItemCode = cur.fetchone()
        idItemCode = rowItemCode['id']
    
        sqlUpdateItemCode = """
        UPDATE `in_01_m_item_codes` 
        SET 
            `description` = %s, 
            `is_foreign` = %s, 
        	`promo_actived` = %s,
        	`active` = %s,
            `update_by` = %s, 
            `update_date` = %s
        WHERE 
           `id` = %s
        """
        cur.execute(sqlUpdateItemCode, (req['description'], 
            req['is_foreign'], req['promo_actived'], req['active'], vToken['id_user'], datetime.date.today(), idItemCode))
            
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), idItemCode, '', 'in_01_m_item_codes', vToken['id_user'])
        
    con.commit()

    sql = """
	SELECT
		sm.stock_code,
        sm.category_code,
        sm.tax_type_item_id,
		sm.description stock_name,
        sm.long_description,
        sm.unit_code,
        sm.mb_flag,
        sm.sales_account,
        sm.cogs_account,
        sm.inventory_account,
        sm.adjustment_account,
        sm.assembly_account,
        sm.is_sale,
        sm.editable,
        sm.default_supplier_id,
        sm.barang_hilang_account,
        sm.bonus_penjualan_account,
        sm.retur_cogs_account,
        sm.retur_sales_account,
		sm.cog_code,
        sm.potongan_account,
        sm.hutang_sementara_account,
        sm.payable_account,
        sm.receivable_account,
        sm.no_sale_android_toko,
        sc.description sc_name,
		cog.description cog_name,
        ttitm.name tt_name,
        iu.name iu_name
	FROM
		`in_03_m_stock_master` sm
        LEFT JOIN `in_02_m_stock_category` sc ON (sc.category_code = sm.category_code)
		LEFT JOIN `in_01_m_class_of_good` cog ON (cog.cog_code = sm.cog_code)
        LEFT JOIN `st_01_item_tax_type` ttitm ON (ttitm.tax_type_item_id = sm.tax_type_item_id)
        LEFT JOIN `in_01_m_item_units` iu ON (iu.code = sm.unit_code)
	WHERE
		sm.`delete_mark` = 0 
		AND sm.`stock_code` = %s
    """
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "stock_code" : row['stock_code'],
        "category_code" : row['category_code'],
        "tax_type_item_id" : row['tax_type_item_id'],
        "stock_name" : row['stock_name'],
        "long_description" : row['long_description'],
        "unit_code" : row['unit_code'],
        "mb_flag" : row['mb_flag'],
        "sales_account" : row['sales_account'],
        "cogs_account" : row['cogs_account'],
        "inventory_account" : row['inventory_account'],
        "adjustment_account" : row['adjustment_account'],
        "assembly_account" : row['assembly_account'],
        "is_sale" : row['is_sale'],
        "editable" : row['editable'],
        "default_supplier_id" : row['default_supplier_id'],
        "barang_hilang_account" : row['barang_hilang_account'],
        "bonus_penjualan_account" : row['bonus_penjualan_account'],
        "retur_cogs_account" : row['retur_cogs_account'],
        "retur_sales_account" : row['retur_sales_account'],
        "cog_code" : row['cog_code'],
        "potongan_account" : row['potongan_account'],
        "hutang_sementara_account" : row['hutang_sementara_account'],
        "payable_account" : row['payable_account'],
        "receivable_account" : row['receivable_account'],
        "no_sale_android_toko" : row['no_sale_android_toko'],
        "sc_name" : row['sc_name'],
        "cog_name" : row['cog_name'],
        "tt_name" : row['tt_name'],
        "iu_name" : row['iu_name']
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    
    #get is_sale
    sqlGetSale = """    
    SELECT 
        is_sale
    FROM 
        `in_03_m_stock_master` 
    WHERE 
        stock_code = %s;
    """
    cur.execute(sqlGetSale, (id))
    rowItemSale = cur.fetchone()
    isSaleItem = rowItemSale[0]
    
    isComboItem = 0
    if (isSaleItem == 1):
        sqlGetItemCode = """    
        SELECT 
            item_code,
            is_combo
        FROM 
            `in_01_m_item_codes` 
        WHERE 
            stock_code = %s;
        """
        cur.execute(sqlGetItemCode, (id))
        rowItemCode = cur.fetchone()
        itemCode = rowItemCode[0]
        isComboItem = rowItemCode[1]
        
        sqlDelete_itemCode = "UPDATE `in_01_m_item_codes` SET delete_by = %s, delete_date = %s, delete_mark = %s WHERE item_code = %s"
        cur.execute(sqlDelete_itemCode, (vToken['id_user'], datetime.date.today(), "1", itemCode))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', 'in_01_m_item_codes', vToken['id_user'])

        if (isComboItem == 1):
            sqlDelete_itemCombo = "UPDATE `in_04_m_item_combo` SET delete_by = %s, delete_date = %s, delete_mark = %s WHERE stock_code = %s"
            cur.execute(sqlDelete_itemCombo, (vToken['id_user'], datetime.date.today(), "1", params['id']))
            inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', 'in_04_m_item_combo', vToken['id_user'])
    
    if (isComboItem == 0):
        sqlDelete_stockMaster = "UPDATE `in_03_m_stock_master` SET delete_by = %s, delete_date = %s, delete_mark = %s WHERE stock_code = %s"
        cur.execute(sqlDelete_stockMaster, (vToken['id_user'], datetime.date.today(), "1", params['id']))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
