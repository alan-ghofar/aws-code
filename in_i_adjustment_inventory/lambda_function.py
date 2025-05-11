import datetime
import valid_token
import valid_menu
import pymysql.cursors
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


tableName = 'in_01_t_adjustment_inquiry'


def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 404)

    global idMenu
    global typeDocument

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
    dataMenu = inc_db.getDataMenu(con, idMenu)
    pathMenunya = dataMenu['path']  # inc_db.getPathMenu(con, idMenu)
    codeDocument = dataMenu['code_doc']
    # return inc_def.send_response_data(codeDocument, 404)
    typeDocument = inc_db.getTypeDocument(con, codeDocument)

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
    else:
        return inc_def.send_response_data([], 404)


# =============================== FUNCTION GET
def functionGet(con, event):
    cur = con.cursor(pymysql.cursors.DictCursor)
    headers = event['headers']
    kodePerusahaan = headers.get('kode_perusahaan')
    params = event['queryStringParameters']

    try:
        if not kodePerusahaan:
            return inc_def.send_response_data("Company not found!", 404)

        sql = """
       SELECT 
            adj.trans_no,
        	adj.type_doc,
        	adj.reference,
        	adj.adj_type,
        	adtype.adjustment_type,
        	adj.jenis_adj,
        	adj.coa_adj,
        	adj.kode_lokasi,
        	lok.nama_lokasi,
        	adj.trans_date,
        	adj.total_value,
        	adj.status_code,
        	dstatus.status_name,
        	adj.kode_perusahaan,
        	per.nama_perusahaan,
        	adj.memo,
        	adj.create_by,
        	adj.create_date,
        	adj_line.id,
        	adj_line.stock_code,
        	adj_line.item_name,
        	adj_line.quantity,
        	adj_line.unit_price,
        	adj_line.item_unit,
        	adj_line.src_id,
        	adj_line.no_ref,
        	adj_line.doc_ref
        FROM 
	        in_01_t_adjustment adj
        	LEFT JOIN in_02_t_adjustment_detail adj_line ON (adj_line.trans_no = adj.trans_no AND adj_line.type_doc = adj.type_doc)
        	LEFT JOIN st_01_perusahaan per ON (per.kode_perusahaan = adj.kode_perusahaan)
        	LEFT JOIN st_01_lokasi lok ON (lok.kode_lokasi = adj.kode_lokasi)
        	LEFT JOIN in_01_m_inventory_adj_types adtype ON (adtype.id = adj.adj_type)
        	LEFT JOIN st_02_document_status dstatus ON (dstatus.status_code = adj.status_code AND dstatus.type_doc = adj.type_doc)
        WHERE
        	adj.kode_perusahaan = %s AND adj.type_doc = %s
        """

        if params is not None:
            if params.get('trans_no') is not None:
                sql += " AND adj.trans_no = " + params.get('trans_no')
            if params.get('start_date') is not None:
                sql += " AND adj.trans_date >= '" + \
                    params.get('start_date') + "'"
            if params.get('end_date') is not None:
                sql += " AND adj.trans_date <= '" + \
                    params.get('end_date') + "'"
            if params.get('adj_type') is not None:
                sql += "AND adj.adj_type = '" + params.get('adj_type') + "'"
            if params.get('jenis_adj') is not None:
                sql += "AND adj.jenis_adj = '" + params.get('jenis_adj') + "'"
        sql += " ORDER BY adj.trans_no, adj_line.id, adj.jenis_adj"

        # return inc_def.send_response_data(sql, 200)

        cur.execute(sql, (kodePerusahaan, typeDocument))
        myresult = cur.fetchall()
        returnData = []
        lineItemData = []
        headerData = []
        temptrans_no = 0
        for row in myresult:
            transno = row['trans_no']
            if temptrans_no != 0 and transno != temptrans_no:
                headerData['line_items'] = lineItemData
                lineItemData = []
                returnData.append(headerData)

            headerData = {
                "trans_no": row['trans_no'],
                "type_doc": row['type_doc'],
                "reference": row['reference'],
                "adj_type": row['adj_type'],
                "adjustment_type": row['adjustment_type'],
                "jenis_adj": row['jenis_adj'],
                "coa_adj": row['coa_adj'],
                "kode_lokasi": row['kode_lokasi'],
                "nama_lokasi": row['nama_lokasi'],
                "trans_date": row['trans_date'],
                "total_value": row['total_value'],
                "status_code": row['status_code'],
                "status_name": row['status_name'],
                "kode_perusahaan": row['kode_perusahaan'],
                "nama_perusahaan": row['nama_perusahaan'],
                "memo": row['memo'],
                "create_by": row['create_by'],
                "create_date": row['create_date']
            }

            linenya = {
                "id": row['id'],
                "stock_code": row['stock_code'],
                "item_name": row['item_name'],
                "quantity": row['quantity'],
                "unit_price": row['unit_price'],
                "item_unit": row['item_unit'],
                "src_id": row['src_id'],
                "no_ref": row['no_ref'],
                "doc_ref": row['doc_ref']
            }

            lineItemData.append(linenya)
            temptrans_no = transno

        if (len(lineItemData) > 0):
            headerData['line_items'] = lineItemData
        lineItemData = []
        returnData.append(headerData)
    except Exception as e:
        return inc_def.send_response_data(str(e), 500)

    cur.close()
    return inc_def.send_response_data(returnData if headerData else [], 200)
