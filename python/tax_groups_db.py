import pymysql
import pymysql.cursors
import includes_definitions as inc_def

def getTaxGroupItemsAsArray(con, taxGroupId):
    returnTaxArray = {}
    taxGroupItems = getTaxGroupRates(con, taxGroupId)
    # return taxGroupItems
    for row in taxGroupItems:
        row['value'] = 0
        returnTaxArray[row['tax_type_id']] = row
        
    return returnTaxArray
    
def getTaxGroupRates(con, taxGroupId):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
    SELECT
	t.tax_type_id,
	CONCAT( t.`name`, ' (', t.rate, '%)' ) AS tax_type_name,
	t.sales_gl_code,
	t.purchasing_gl_code,
    IF( g.tax_type_id, t.rate, NULL ) AS rate,
	t.code_tax_category 
FROM
	st_02_tax_types t
	LEFT JOIN st_03_tax_group_items g 
	    ON ( t.tax_type_id = g.tax_type_id 
	    AND g.active = 1 
	    AND g.delete_mark = 0 
	    AND g.tax_group_id = """
    sql += str(taxGroupId) + ") WHERE t.active = 1 AND t.delete_mark = 0"
	
    cur.execute(sql)
    myResult = cur.fetchall()
    taxGroupItem = []
    for row in myResult:
        taxGroupItem.append(row)

    cur.close()
    return taxGroupItem


def getTaxCategoryIncluded(con, idTaxIncluded):
    cur = con.cursor(pymysql.cursors.DictCursor)
    taxIncluded = []
    sql = """
    SELECT * FROM st_02_tax_category_included WHERE id_tax_included = %s
    AND active = 1 AND delete_mark = 0
    """
    cur.execute(sql, (idTaxIncluded))
    myResult = cur.fetchall()
    for row in myResult:
        taxIncluded.append(row['code_tax_category'])
    
    return taxIncluded


def getItemTaxTypeForItem(con, stockId):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
    SELECT txitm.* 
    FROM st_01_item_tax_type txitm, in_03_m_stock_master stk 
    WHERE stk.stock_code = %s 
	AND txitm.tax_type_item_id = stk.tax_type_item_id
	AND txitm.active = 1 
	AND txitm.delete_mark = 0
    """
    cur.execute(sql, (stockId))
    row = cur.fetchone()
    return row

def getItemTaxTypeForItemById(con, taxTypeItemId):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
    SELECT txitm.* 
    FROM st_01_item_tax_type txitm
    WHERE txitm.tax_type_item_id = %s 
	AND txitm.active = 1 
	AND txitm.delete_mark = 0
    """
    cur.execute(sql, (taxTypeItemId))
    row = cur.fetchone()
    return row


def getItemTaxTypeExemptions(con, taxTypeItemId):
    cur = con.cursor(pymysql.cursors.DictCursor)
    taxItemExempt = []
    sql = """
    SELECT * FROM st_03_tax_item_exemptions WHERE tax_type_item_id=%s and active = 1 and delete_mark = 0
    """
    cur.execute(sql, (taxTypeItemId))
    myResult = cur.fetchall()
    for row in myResult:
        taxItemExempt.append(row)
    
    return taxItemExempt