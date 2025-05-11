import pymysql.cursors
import datetime
import dateutil.tz


# ambil type_doc berdasarkan code_doc cek table st_01_document
def getTypeDocument(con, code_doc):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT type_doc FROM st_01_document WHERE code_doc = %s"
    cur.execute(sql, (code_doc))
    row = cur.fetchone()
    type_document = row['type_doc']
    cur.close()
    if row['type_doc'] == 0:
        return "false"
    else:
        return type_document


# ambil path menu berdasarkan gcode cek table sc_02_menu
def getPathMenu(con, idMenu):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT path  FROM sc_02_menu WHERE id =  %s"
    cur.execute(sql, (idMenu))
    row = cur.fetchone()
    pathMenu = row['path']
    cur.close()
    if row['path'] == 0:
        return "false"
    else:
        return pathMenu


def getDataMenu(con, idMenu):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT * FROM sc_02_menu WHERE id =  %s"
    cur.execute(sql, (idMenu))
    row = cur.fetchone()
    dataMenu = row
    cur.close()
    if row['path'] == 0:
        return "false"
    else:
        return dataMenu


# insert ke table sc_01_audit_master
def addAuditMaster(con, idMenu, inAction, inMasterId, inKeterangan, inNamaTable, inUserId):
    tz_JKT = dateutil.tz.gettz('Asia/Jakarta')

    cur = con.cursor()
    dataMenunya = getDataMenu(con, idMenu)
    sqlInsert = """
	INSERT INTO sc_01_audit_master
	(id_menu, nama_menu, action, keterangan, tanggal,
	 user_id, id_master, nama_master, nama_tabel)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
	"""
    cur.execute(sqlInsert, (dataMenunya['id'], dataMenunya['nama_menu'], inAction,
                            inKeterangan, datetime.datetime.now(tz_JKT), inUserId, inMasterId, dataMenunya['nama_menu'], inNamaTable))
    # con.commit()
    # cur.close()
    return True


def getIdMenuByTableName(con, tableName):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
	SELECT id
	FROM sc_02_menu
	WHERE table_name = %s
	AND active = 1  AND delete_mark = 0
	"""
    cur.execute(sql, (tableName))
    row = cur.fetchone()

    cur.close()
    if row['id'] == 0:
        return "false"
    else:
        return row['id']

#validasi cek double data
def checkDoubleData(cur, field, table):
    sql = "SELECT COUNT(*) FROM " + table + " WHERE delete_mark = 0"

    for column, value in field.items():
        sql += " AND `" + column + "` = '" + str(value) + "'"

    cur.execute(sql)
    row = cur.fetchone()
    if row[0] == 0:
        return "false"
    else:
        return "true"

#cek keberadaan status document di master document status berdasarkan type document
def isValidDocumentStatus(con, type_doc, status_code):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
    SELECT
    	COUNT( type_doc ) hitung 
    FROM
    	st_02_document_status 
    WHERE
    	delete_mark = 0 
    	AND active = 1
    	AND type_doc = %s
    	AND `status_code` = %s
    """
    cur.execute(sql, (type_doc, status_code))
    row = cur.fetchone()
    
    return True if row['hitung'] > 0 else False

#cek ke master doc workflow, penanda type document tsb. menggunakan approval atau tidak
def isUsingApprovalDocument(con, type_doc):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
    SELECT
    	COUNT( type_doc ) hitung 
    FROM
    	st_02_document_workflow 
    WHERE
    	delete_mark = 0 
    	AND active = 1
    	AND type_doc = %s
    """
    cur.execute(sql, (type_doc))
    row = cur.fetchone()
    
    return True if row['hitung'] > 0 else False


# GET VALUE DARI SYSPREFS
def getValueSysPrefs(name, con) :
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT nilai FROM `st_sys_prefs` WHERE kode = %s"
    cur.execute(sql, (name))
    row = cur.fetchone()
    return row['nilai']
