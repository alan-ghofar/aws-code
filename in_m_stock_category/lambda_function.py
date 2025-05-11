import json
import datetime
import valid_token
import valid_menu
import pymysql.cursors
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

tableName = 'in_02_m_stock_category'

def lambda_handler(event, context):
    #return inc_def.send_response_data(event, 200)
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
    cur = con.cursor(pymysql.cursors.DictCursor)
    params = event['queryStringParameters']
    filterCategoryCode = '%'
    sql = """
	SELECT
		sc.category_code,
		sc.description category_name,
		sc.parent_category,
        scparent.description AS parent_category_name,
		sc.cog_code,
		cog.description cog_name,
        sc.`sales_account`,
        coa_sales.account_name as sales_account_name,
        sc.`cogs_account`,
        coa_cogs.account_name as cogs_account_name,
        sc.`inventory_account`, 
        coa_inventory.account_name as inventory_account_name,
        sc.`adjustment_account`, 
        coa_adjustment.account_name as adjustment_account_name,
        sc.`assembly_account`,
        coa_assembly.account_name as assembly_account_name,
        sc.`barang_hilang_account`,
        coa_barang_hilang.account_name as barang_hilang_account_name,
        sc.`bonus_penjualan_account`, 
        coa_bonus_penjualan.account_name as bonus_penjualan_account_name,
        sc.`retur_cogs_account`,
        coa_retur_cogs.account_name as retur_cogs_account_name,
        sc.`retur_sales_account`,
        coa_retur_sales.account_name as retur_sales_account_name,
        sc.`potongan_account`,
        coa_potongan.account_name as potongan_account_name,
        sc.`hutang_sementara_account`,
        coa_hutang_sementara.account_name as hutang_sementara_account_name,
        sc.`payable_account`,
        coa_payable.account_name as payable_account_name,
        sc.`receivable_account`,
        coa_receivable.account_name as receivable_account_name
	FROM
		`in_02_m_stock_category` sc
		LEFT JOIN `in_01_m_class_of_good` cog ON (cog.cog_code = sc.cog_code)
        LEFT JOIN in_02_m_stock_category scparent ON (scparent.category_code = sc.parent_category)
        LEFT JOIN  fi_03_m_chart_of_account  coa_sales ON (coa_sales.account_code = sc.sales_account)
        LEFT JOIN  fi_03_m_chart_of_account  coa_cogs ON (coa_cogs.account_code = sc.sales_account)
        LEFT JOIN  fi_03_m_chart_of_account  coa_inventory ON (coa_inventory.account_code = sc.sales_account)
        LEFT JOIN  fi_03_m_chart_of_account  coa_adjustment ON (coa_adjustment.account_code = sc.sales_account)
        LEFT JOIN  fi_03_m_chart_of_account  coa_assembly ON (coa_assembly.account_code = sc.sales_account)
        LEFT JOIN  fi_03_m_chart_of_account  coa_barang_hilang ON (coa_barang_hilang.account_code = sc.sales_account)
        LEFT JOIN  fi_03_m_chart_of_account  coa_bonus_penjualan ON (coa_bonus_penjualan.account_code = sc.sales_account)
        LEFT JOIN  fi_03_m_chart_of_account  coa_retur_cogs ON (coa_retur_cogs.account_code = sc.sales_account)
        LEFT JOIN  fi_03_m_chart_of_account  coa_retur_sales ON (coa_retur_sales.account_code = sc.sales_account)
        LEFT JOIN  fi_03_m_chart_of_account  coa_potongan ON (coa_potongan.account_code = sc.sales_account)
        LEFT JOIN  fi_03_m_chart_of_account  coa_hutang_sementara ON (coa_hutang_sementara.account_code = sc.sales_account)
        LEFT JOIN  fi_03_m_chart_of_account  coa_payable ON (coa_payable.account_code = sc.sales_account)
        LEFT JOIN  fi_03_m_chart_of_account  coa_receivable ON (coa_receivable.account_code = sc.sales_account)
	WHERE
		sc.`delete_mark` = 0 
		AND sc.`active` = 1
        AND sc.category_code LIKE %s
    """
    if params is not None:
        if params.get('id'):
            filterCategoryCode = params.get('id')

    cur.execute(sql, (filterCategoryCode))
    myresult = cur.fetchall()

    allData = []
    for row in myresult:
        data = {
            "category_code": row['category_code'],
            "category_name": row['category_name'],
            "parent_category": row['parent_category_name'],
            "cog_code": row['cog_code'],
            "cog_name": row['cog_name'],
            "sales_account": row['sales_account'],
            "sales_account_name": row['sales_account_name'],
            "cogs_account": row['cogs_account'],
            "cogs_account_name": row['cogs_account_name'],
            "inventory_account": row['inventory_account'],
            "inventory_account_name": row['inventory_account_name'],
            "adjustment_account": row['adjustment_account'],
            "adjustment_account_name": row['adjustment_account_name'],
            "assembly_account": row['assembly_account'],
            "assembly_account_name": row['assembly_account_name'],
            "barang_hilang_account": row['barang_hilang_account'],
            "barang_hilang_account_name": row['barang_hilang_account_name'],

            "bonus_penjualan_account": row['bonus_penjualan_account'],
            "bonus_penjualan_account_name": row['bonus_penjualan_account_name'],
            "retur_cogs_account": row['retur_cogs_account'],
            "retur_cogs_account_name": row['retur_cogs_account_name'],
            "retur_sales_account": row['retur_sales_account'],
            "retur_sales_account_name": row['retur_sales_account_name'],
            "potongan_account": row['potongan_account'],
            "potongan_account_name": row['potongan_account_name'],
            "hutang_sementara_account": row['hutang_sementara_account'],
            "hutang_sementara_account_name": row['hutang_sementara_account_name'],
            "payable_account": row['payable_account'],
            "payable_account_name": row['payable_account_name'],
            "receivable_account": row['receivable_account'],
            "receivable_account_name": row['receivable_account_name'],
        }
        allData.append(data)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    desc_category = req['description']

    # cek deskripsi tidak boleh sama
    sql = """
	SELECT
		COUNT(*) as ada
	FROM
		`in_02_m_stock_category` sc
	WHERE
		sc.`delete_mark` = 0 
		AND sc.`active` = 1
        AND sc.description = %s
    """
    if desc_category is not None:
        filterCategoryDesc = desc_category

    cur.execute(sql, (filterCategoryDesc))
    hasil = cur.fetchone()
    adatidak = hasil['ada']
    if adatidak is not None:
        return inc_def.send_response_data("Category name already exists", 500)
    
    sqlInsert = """
    INSERT INTO `in_02_m_stock_category` 
        ( parent_category, cog_code, description, 
          `sales_account`,
          `cogs_account`,
          `inventory_account`,
          `adjustment_account`,
          `assembly_account`,
          `barang_hilang_account`,
          `bonus_penjualan_account`,
          `retur_cogs_account`,
          `retur_sales_account`,
          `potongan_account`,
          `hutang_sementara_account`,
          `payable_account`,
          `receivable_account`,
         active, create_by, create_date, delete_mark ) 
    VALUES 
        (%s, %s, %s, 
         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, 0)
    """
    cur.execute(sqlInsert, (req['parent_category'], req['cog_code'], req['description'], 
        req['sales_account'], req['cogs_account'], req['inventory_account'], req['adjustment_account'], req['assembly_account'], 
        req['barang_hilang_account'], req['bonus_penjualan_account'], req['retur_cogs_account'], req['retur_sales_account'], 
        req['potongan_account'], req['hutang_sementara_account'], req['payable_account'], req['receivable_account'], 
        req['active'], vToken['id_user'], datetime.date.today()))
    id = con.insert_id()
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    con.commit()

    sql = """
	SELECT
		sc.category_code,
		sc.description category_name,
		sc.parent_category,
		sc.cog_code,
		cog.description cog_name
	FROM
		`in_02_m_stock_category` sc
		LEFT JOIN `in_01_m_class_of_good` cog ON (cog.cog_code = sc.cog_code)
	WHERE
		sc.`delete_mark` = 0 
		AND sc.`active` = 1
		AND sc.category_code = %s
    """
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "category_code": row['category_code'],
        "category_name": row['category_name'],
        "parent_category": row['parent_category'],
        "cog_code": row['cog_code'],
        "cog_name": row['cog_name']
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
    desc_category = req['description']
    
    # cek category_name sudah ada belum
    sql = """
	SELECT
		COUNT(*) as ada
	FROM
		`in_02_m_stock_category` sc
	WHERE
		sc.`delete_mark` = 0 
		AND sc.`active` = 1
        AND sc.description = %s
    """
    if desc_category is not None:
        filterCategoryDesc = desc_category

    cur.execute(sql, (filterCategoryDesc))
    hasil = cur.fetchone()
    adatidak = hasil['ada']
    if adatidak is not None:
        return inc_def.send_response_data("Category name already exists", 500)
    
    sqlUpdate = """
    UPDATE `in_02_m_stock_category` 
    SET 
        `parent_category` = %s, 
        `cog_code` = %s, 
        `description` = %s, 
        
          `sales_account` = %s,
          `cogs_account` = %s,
          `inventory_account` = %s,
          `adjustment_account` = %s,
          `assembly_account` = %s,
          `barang_hilang_account` = %s,
          `bonus_penjualan_account` = %s,
          `retur_cogs_account` = %s,
          `retur_sales_account` = %s,
          `potongan_account` = %s,
          `hutang_sementara_account` = %s,
          `payable_account` = %s,
          `receivable_account` = %s,
          
        `update_by` = %s, 
        `update_date` = %s, 
        `active` = %s
    WHERE 
       `category_code` = %s
    """
    cur.execute(sqlUpdate, (req['parent_category'], req['cog_code'], req['description'], 
        req['sales_account'], req['cogs_account'], req['inventory_account'], req['adjustment_account'],
        req['assembly_account'], req['barang_hilang_account'], req['bonus_penjualan_account'],
        req['retur_cogs_account'], req['retur_sales_account'], req['potongan_account'],
        req['hutang_sementara_account'], req['payable_account'], req['receivable_account'],
        vToken['id_user'], datetime.date.today(), req['active'], params['id']))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    con.commit()

    sql = """
	SELECT
		sc.category_code,
		sc.description category_name,
		sc.parent_category,
		scparent.description parent_category_name,
		sc.cog_code,
		cog.description cog_name
	FROM
		`in_02_m_stock_category` sc
		LEFT JOIN `in_01_m_class_of_good` cog ON (cog.cog_code = sc.cog_code)
		left join in_02_m_stock_category scparent on (scparent.category_code = sc.parent_category)
	WHERE
		sc.`delete_mark` = 0 
		AND sc.`active` = 1
		AND sc.category_code = %s
    """
    cur.execute(sql, (params['id']))
    row = cur.fetchone()
    data = {
        "category_code": row['category_code'],
        "category_name": row['category_name'],
        "parent_category": row['parent_category_name'],
        "cog_code": row['cog_code'],
        "cog_name": row['cog_name']
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    sqlUpdate = "UPDATE `in_02_m_stock_category` SET delete_by = %s, delete_date = %s, delete_mark = %s WHERE category_code = %s"
    cur.execute(sqlUpdate, (vToken['id_user'], datetime.date.today(), "1", id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    con.commit()
    
    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
    