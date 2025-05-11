import json
import datetime
import valid_token
import valid_menu
import pymysql.cursors
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

tableName = 'in_01_t_opname_inquiry'


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
        	op.trans_no,
        	op.type_doc,
        	op.reference,
        	op.coa_opname,
        	op.kode_lokasi,
        	lok.nama_lokasi,
        	op.trans_date,
        	op.status_code,
        	dstatus.status_name,
        	op.kode_perusahaan,
        	per.nama_perusahaan,
        	op.memo,
        	op.create_by,
        	op.create_date,
        	op_line.id,
        	op_line.stock_code,
        	op_line.item_name,
        	op_line.qty_sistem,
        	op_line.qty_opname,
        	op_line.qty_posted,
        	op_line.status_opname,
        	op_line.unit_price,
        	op_line.item_unit
        FROM 
        	`in_01_t_opname` op
        	LEFT JOIN in_02_t_opname_detail op_line ON (op_line.trans_no = op.trans_no AND op_line.type_doc = op.type_doc)
        	LEFT JOIN st_01_perusahaan per ON (per.kode_perusahaan = op.kode_perusahaan)
        	LEFT JOIN st_01_lokasi lok ON (lok.kode_lokasi = op.kode_lokasi)
        	LEFT JOIN st_02_document_status dstatus ON (dstatus.status_code = op.status_code AND dstatus.type_doc = op.type_doc)
        WHERE
        	op.kode_perusahaan = %s AND op.type_doc = %s
        """

        if params is not None:
            if params.get('trans_no') is not None:
                sql += " AND op.trans_no = " + params.get('trans_no')
            if params.get('start_date') is not None:
                sql += " AND op.trans_date >= '" + \
                    params.get('start_date') + "'"
            if params.get('end_date') is not None:
                sql += " AND op.trans_date <= '" + params.get('end_date') + "'"
        sql += " ORDER BY op.trans_no, op_line.id"

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
                "kode_lokasi": row['kode_lokasi'],
                "nama_lokasi": row['nama_lokasi'],
                "trans_date": row['trans_date'],
                "status_code": row['status_code'],
                "status_name": row['status_code'],
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
                "qty_sistem": row['qty_sistem'],
                "qty_opname": row['qty_opname'],
                "qty_posted": row['qty_posted'],
                "status_opname": row['status_opname'],
                "unit_price": row['unit_price'],
                "item_unit": row['item_unit']
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
