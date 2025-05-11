import json
import datetime
import valid_token
import valid_menu
import pymysql.cursors
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


tableName = 'in_01_t_location_transfer_in_inquiry'


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
            lt.trans_no,
        	lt.type_doc,
        	lt.trans_date,
        	lt.from_loc,
        	lt.to_loc,
        	lt.no_asset,
        	lt.isship,
        	lt.status_code,
        	dstatus.status_name,
        	lt.status_transfer,
        	lt.memo,
        	lt.doc_ref,
        	lt.no_ref,
        	lt.reference,
        	lt.date_complete,
        	lt.to_cabang,
        	lt.kode_perusahaan,
        	per.nama_perusahaan,
        	lt.from_cabang,
        	lt.dispatched,
        	lt.create_by,
        	lt.create_date,
        	lt_line.id,
        	lt_line.stock_code,
        	lt_line.item_name,
        	lt_line.quantity,
        	lt_line.quantity_sent,
        	lt_line.unit_price,
        	lt_line.kode_lokasi,
        	lt_line.item_unit,
        	lt_line.src_id
        FROM 
	        `in_01_t_location_transfer` lt
        	LEFT JOIN in_02_t_location_transfer_detail lt_line ON (lt_line.trans_no = lt.trans_no AND lt_line.type_doc = lt.type_doc)
        	LEFT JOIN st_01_perusahaan per ON (per.kode_perusahaan = lt.kode_perusahaan)
        	LEFT JOIN st_02_document_status dstatus ON (dstatus.status_code = lt.status_code AND dstatus.type_doc = lt.type_doc)
        WHERE
        	lt.doc_ref > 0
        	AND lt.no_ref > 0 
        	AND lt.kode_perusahaan = %s
        	AND lt.type_doc = %s
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
        sql += " ORDER BY lt.trans_no, lt_line.id"

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
                "trans_date": row['trans_date'],
                "from_loc": row['from_loc'],
                "to_loc": row['to_loc'],
                "no_asset": row['no_asset'],
                "isship": row['isship'],
                "status_code": row['status_code'],
                "status_name": row['status_name'],
                "status_transfer": row['status_transfer'],
                "memo": row['memo'],
                "doc_ref": row['doc_ref'],
                "no_ref": row['no_ref'],
                "reference": row['reference'],
                "date_complete": row['date_complete'],
                "to_cabang": row['to_cabang'],
                "kode_perusahaan": row['kode_perusahaan'],
                "nama_perusahaan": row['nama_perusahaan'],
                "from_cabang": row['from_cabang'],
                "dispatched": row['dispatched'],
                "create_by": row['create_by'],
                "create_date": row['create_date']
            }

            linenya = {
                "id": row['id'],
                "stock_code": row['stock_code'],
                "item_name": row['item_name'],
                "quantity": row['quantity'],
                "quantity_sent": row['quantity_sent'],
                "unit_price": row['unit_price'],
                "kode_lokasi": row['kode_lokasi'],
                "item_unit": row['item_unit'],
                "src_id": row['src_id']
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
