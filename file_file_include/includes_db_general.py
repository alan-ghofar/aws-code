import pymysql.cursors
import datetime

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

#insert ke table sc_01_audit_master
def addAuditMaster(con, idMenu, inAction, inMasterId, inKeterangan, inNamaTable, inUserId):
	cur = con.cursor()
	dataMenunya = getDataMenu(con, idMenu)
	sqlInsert = """
	INSERT INTO sc_01_audit_master 
	(id_menu, nama_menu, action, keterangan, tanggal, user_id, id_master, nama_master, nama_tabel) 
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
	"""
	cur.execute(sqlInsert, (dataMenunya['id'], dataMenunya['nama_menu'], inAction, 
	inKeterangan, datetime.datetime.today(), inUserId, inMasterId, dataMenunya['nama_menu'], inNamaTable))
	#con.commit()
	#cur.close()
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