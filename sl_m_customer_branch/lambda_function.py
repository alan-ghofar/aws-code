import json
import datetime
import pymysql.cursors
import valid_token as v_token
import valid_menu as v_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db
import dateutil.tz


tableName = 'sl_03_m_customer_branch'


def lambda_handler(event, context):
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
				return inc_def.send_response_data("Forbidden access for this menu ", 403)
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

	sql = """
	SELECT
	br.id,
	br.branch_code,
	br.customer_code,
	br.branch_ref,
	br.br_name,
	br.br_address,
	br.provinsi_code,
	prov.description provinsi,
	br.kabupaten_code,
	kab.description kabupaten,
	br.kecamatan_code,
	kec.description kecamatan,
	br.contact_name,
	br.kode_lokasi,
	lok.nama_lokasi,
	br.tax_group_id,
	taxg.`name` tax_group,
	br.sales_account,
	br.sales_discount_account,
	br.receivables_account,
	br.payment_discount_account,
	br.default_ship_via,
	br.disable_trans,
	br.br_post_address,
	br.notes,
	br.lat,
	br.lng,
	br.keakuratan,
	br.nppkp,
	br.alamat_nppkp,
	br.nama_nppkp,
	br.ktp,
	br.alamat_ktp 
FROM
	sl_03_m_customer_branch br
	LEFT JOIN sl_03_m_kecamatan kec ON ( br.kecamatan_code = kec.kecamatan_code AND kec.delete_mark = 0 )
	LEFT JOIN sl_01_m_provinsi prov ON ( br.provinsi_code = prov.provinsi_code AND prov.delete_mark = 0 )
	LEFT JOIN sl_02_m_kabupaten kab ON ( br.kabupaten_code = kab.kabupaten_code AND kab.delete_mark = 0 )
	LEFT JOIN st_01_lokasi lok ON ( br.kode_lokasi = lok.kode_lokasi AND lok.delete_mark = 0 )
	LEFT JOIN st_01_tax_groups taxg ON ( taxg.tax_group_id = br.tax_group_id AND taxg.delete_mark = 0 ) 
WHERE
	br.delete_mark = 0 
	AND br.active = 1 
	"""
	
	if params is not None:
		if params.get('id'):
			sql += " AND br.id = '" + params.get('id') + "'"
		if params.get('customer_code'):
			sql += " AND br.customer_code = '" + params.get('customer_code') + "'"

	cur.execute(sql)
	result = cur.fetchall()

	allData = []
	for row in result:
		data = {
			"id": row['id'],
			"branch_code": row['branch_code'],
			"customer_code": row['customer_code'],
			"branch_ref": row['branch_ref'],
			"br_name": row['br_name'],
			"br_address": row['br_address'],
			"provinsi_code": row['provinsi_code'],
			"provinsi": row['provinsi'],
			"kabupaten_code": row['kabupaten_code'],
			"kabupaten": row['kabupaten'],
			"kecamatan_code": row['kecamatan_code'],
			"kecamatan": row['kecamatan'],
			"contact_name": row['contact_name'],
			"kode_lokasi": row['kode_lokasi'],
			"nama_lokasi": row['nama_lokasi'],
			"tax_group_id": row['tax_group_id'],
			"tax_group": row['tax_group'],
			"sales_account": row['sales_account'],
			"sales_discount_account": row['sales_discount_account'],
			"receivables_account": row['receivables_account'],
			"payment_discount_account": row['payment_discount_account'],
			"default_ship_via": row['default_ship_via'],
			"disable_trans": row['disable_trans'],
			"br_post_address": row['br_post_address'],
			"notes": row['notes'],
			#"grade_id": row['grade_id'],
			"lat": row['lat'],
			"lng": row['lng'],
			"keakuratan": row['keakuratan'],
			"nppkp": row['nppkp'],
			"alamat_nppkp": row['alamat_nppkp'],
			"nama_nppkp": row['nama_nppkp'],
			"ktp": row['ktp'],
			"alamat_ktp": row['alamat_ktp']
		}
		allData.append(data)

	cur.close()
	return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
	con.begin()
	cur = con.cursor(pymysql.cursors.DictCursor)
	req = json.loads(event['body'])
	vToken = event['data_user']
	customerCode = req['customer_code']
	
	try:
		#ambil jumlah branch berdasarkan customer code
		sqlbranch = """
		SELECT COUNT( id )+1 next_branch_code 
		FROM sl_03_m_customer_branch 
		WHERE customer_code = %s AND delete_mark = 0
		"""
		cur.execute(sqlbranch, (customerCode))
		rowBranch = cur.fetchone()
		branchCode = rowBranch['next_branch_code']
		
		sqlInsert = """
		INSERT INTO sl_03_m_customer_branch 
		(branch_code, customer_code, br_name, branch_ref, br_address, 
		kecamatan_code, provinsi_code, kabupaten_code, 
		contact_name, kode_lokasi, tax_group_id, 
		sales_account, sales_discount_account, receivables_account, payment_discount_account,
		default_ship_via, disable_trans, br_post_address,  notes, 
		nppkp, nama_nppkp, alamat_nppkp, ktp, alamat_ktp, lat, lng,  keakuratan, 
		create_by, create_date, active, delete_mark)
		VALUES
		(%s, %s, %s, %s, %s, 
		%s, %s, %s,
		%s, %s, %s,
		%s, %s, %s, %s,
		%s, %s, %s, %s, 
		%s, %s, %s, %s, %s, %s, %s, %s,
		%s, %s, %s, %s
		)
		"""
		cur.execute(sqlInsert, (branchCode, customerCode, req['br_name'], req['branch_ref'], req['br_address'],
				req['kecamatan_code'], req['provinsi_code'], req['kabupaten_code'], 
				req['contact_name'], req['kode_lokasi'], req['tax_group_id'],
				req['sales_account'], req['sales_discount_account'], req['receivables_account'], req['payment_discount_account'], 
				req['default_ship_via'], req['disable_trans'], req['br_post_address'],  req['notes'], 
				req['nppkp'], req['nama_nppkp'], req['alamat_nppkp'], req['ktp'], req['alamat_ktp'], req['lat'], req['lng'], req['keakuratan'],
				vToken['id_user'], datetime.datetime.now(tz_JKT), 1, 0))
		id = con.insert_id()
		inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
		con.commit()
		
		sql = """
		SELECT
			br.id,
			br.branch_code,
			br.customer_code,
			br.branch_ref,
			br.br_name,
			br.br_address,
			br.provinsi_code,
			prov.description provinsi,
			br.kabupaten_code,
			kab.description kabupaten,
			br.kecamatan_code,
			kec.description kecamatan,
			br.contact_name,
			br.kode_lokasi,
			lok.nama_lokasi,
			br.tax_group_id,
			taxg.`name` tax_group,
			br.sales_account,
			br.sales_discount_account,
			br.receivables_account,
			br.payment_discount_account,
			br.default_ship_via,
			br.disable_trans,
			br.br_post_address,
			br.notes,
			br.lat,
			br.lng,
			br.keakuratan,
			br.nppkp,
			br.alamat_nppkp,
			br.nama_nppkp,
			br.ktp,
			br.alamat_ktp
		FROM
		sl_03_m_customer_branch br
		LEFT JOIN sl_03_m_kecamatan kec ON ( br.kecamatan_code = kec.kecamatan_code AND kec.delete_mark = 0 )
		LEFT JOIN sl_01_m_provinsi prov ON ( br.provinsi_code = prov.provinsi_code AND prov.delete_mark = 0 )
		LEFT JOIN sl_02_m_kabupaten kab ON ( br.kabupaten_code = kab.kabupaten_code AND kab.delete_mark = 0 )
		LEFT JOIN st_01_lokasi lok ON ( br.kode_lokasi = lok.kode_lokasi AND lok.delete_mark = 0 )
		LEFT JOIN st_01_tax_groups taxg ON ( taxg.tax_group_id = br.tax_group_id AND taxg.delete_mark = 0 ) 
		WHERE customer_code = %s AND branch_code = %s
		"""
		cur.execute(sql, (customerCode, branchCode))
		row = cur.fetchone()
		data = {
				"branch_code": row['branch_code'],
				"customer_code": row['customer_code'],
				"branch_ref": row['branch_ref'],
				"br_name": row['br_name'],
				"br_address": row['br_address'],
				"provinsi_code": row['provinsi_code'],
				"provinsi": row['provinsi'],
				"kabupaten_code": row['kabupaten_code'],
				"kabupaten": row['kabupaten'],
				"kecamatan_code": row['kecamatan_code'],
				"kecamatan": row['kecamatan'],
				"contact_name": row['contact_name'],
				"kode_lokasi": row['kode_lokasi'],
				"nama_lokasi": row['nama_lokasi'],
				"tax_group_id": row['tax_group_id'],
				"tax_group": row['tax_group'],
				"sales_account": row['sales_account'],
				"sales_discount_account": row['sales_discount_account'],
				"receivables_account": row['receivables_account'],
				"payment_discount_account": row['payment_discount_account'],
				"default_ship_via": row['default_ship_via'],
				"disable_trans": row['disable_trans'],
				"br_post_address": row['br_post_address'],
				"notes": row['notes'],
				#"grade_id": row['grade_id'],
				"lat": row['lat'],
				"lng": row['lng'],
				"keakuratan": row['keakuratan'],
				"nppkp": row['nppkp'],
				"alamat_nppkp": row['alamat_nppkp'],
				"nama_nppkp": row['nama_nppkp'],
				"ktp": row['ktp'],
				"alamat_ktp": row['alamat_ktp']
		}
		return inc_def.send_response_data(data, 200)
	except KeyError as e:
		return inc_def.send_response_data("key error", 200)
	# except TypeError as e:
		
	finally:
		cur.close()
	


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
		UPDATE sl_03_m_customer_branch 
			SET br_name = %s,
			branch_ref = %s,
			br_address = %s,
			kecamatan_code = %s,
			provinsi_code = %s,
			kabupaten_code = %s,
			contact_name = %s,
			kode_lokasi = %s,
			tax_group_id = %s,
			sales_account = %s,
			sales_discount_account = %s,
			receivables_account = %s,
			payment_discount_account = %s,
			default_ship_via = %s,
			disable_trans = %s,
			br_post_address = %s,
			
			notes = %s,
			nppkp = %s,
			nama_nppkp = %s,
			alamat_nppkp = %s,
			ktp = %s,
			alamat_ktp = %s,
			lat = %s,
			lng = %s,
			keakuratan = %s,
			update_by = %s,
			update_date = %s,
			active = %s 
			WHERE
				id = %s
		"""
		
		cur.execute(sqlUpdate, (req['br_name'], req['branch_ref'], req['br_address'],
				req['kecamatan_code'], req['provinsi_code'], req['kabupaten_code'], 
				req['contact_name'], req['kode_lokasi'], req['tax_group_id'],
				req['sales_account'], req['sales_discount_account'], req['receivables_account'], req['payment_discount_account'], 
				req['default_ship_via'], req['disable_trans'], req['br_post_address'], req['notes'], 
				req['nppkp'], req['nama_nppkp'], req['alamat_nppkp'], req['ktp'], req['alamat_ktp'], req['lat'], req['lng'], req['keakuratan'],
				vToken['id_user'], datetime.datetime.now(tz_JKT), req['active'], id))
		inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
	except Exception as e:
		con.rollback()
		con.close()
		return inc_def.send_response_data(str(e), 500)
	else:
		con.commit()

	sql = """
	SELECT customer_code, branch_code, br_name, branch_ref, br_address 
	FROM sl_03_m_customer_branch 
	WHERE id = %s
	"""
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
	cur = con.cursor()
	vToken = event['data_user']
	params = event['queryStringParameters']
	id = params['id']
	sqlDelete = """
	UPDATE `sl_03_m_customer_branch` 
	SET 
		delete_by = %s, 
		delete_date = %s, 
		delete_mark = 1 
	WHERE 
		id = %s
	"""
	
	cur.execute(sqlDelete, (vToken['id_user'], datetime.datetime.now(tz_JKT), id))
	inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
	con.commit()
	
	data = {
		"message": "Deleted!"
	}
	
	cur.close()
	return inc_def.send_response_data(data, 200)