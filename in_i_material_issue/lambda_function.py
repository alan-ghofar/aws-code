import json
import datetime
import valid_token
import valid_menu
import pymysql.cursors
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db


tableName = 'in_01_t_material_issue_inqury'


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
                # return inc_def.send_response_data(pathFull, 403)
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
        	mi.trans_no,
        	mi.type_doc,
        	mi.mat_issue_type,
        	mi_type.coa,
        	mi.kode_lokasi,
        	lok.nama_lokasi,
        	mi.kode_perusahaan,
        	per.nama_perusahaan,
        	mi.coa_adj,
        	mi.trans_date,
        	mi.total_value,
        	mi.status_code,
        	dstatus.status_name,
        	mi.memo,
        	mi.create_by,
        	mi.create_date,
        	mi_line.id,
        	mi_line.stock_code,
        	mi_line.item_name,
        	mi_line.quantity,
        	mi_line.unit_price
        FROM
        	in_01_t_material_issue mi
        	LEFT JOIN in_02_t_material_issue_detail mi_line ON (mi_line.type_doc = mi.type_doc AND mi_line.trans_no = mi.trans_no)
        	LEFT JOIN in_01_m_material_issue_types mi_type ON (mi_type.id = mi.mat_issue_type)
        	LEFT JOIN st_01_perusahaan per ON (per.kode_perusahaan = mi.kode_perusahaan)
        	LEFT JOIN st_01_lokasi lok ON (lok.kode_lokasi = mi.kode_lokasi)
        	LEFT JOIN st_02_document_status dstatus ON (dstatus.status_code = mi.status_code AND dstatus.type_doc = mi.type_doc)
        WHERE
        	mi.kode_perusahaan = %s AND mi.type_doc = %s
        """

        if params is not None:
            if params.get('trans_no') is not None:
                sql += " AND mi.trans_no = " + params.get('trans_no')
            if params.get('start_date') is not None:
                sql += " AND mi.trans_date >= '" + \
                    params.get('start_date') + "'"
            if params.get('end_date') is not None:
                sql += " AND mi.trans_date <= '" + params.get('end_date') + "'"
        sql += " ORDER BY mi.trans_no, mi_line.id"

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
                "mat_issue_type": row['mat_issue_type'],
                "coa": row['coa'],
                "kode_lokasi": row['kode_lokasi'],
                "nama_lokasi": row['nama_lokasi'],
                "kode_perusahaan": row['kode_perusahaan'],
                "coa_adj": row['coa_adj'],
                "trans_date": row['trans_date'],
                "total_value": row['total_value'],
                "status_code": row['status_code'],
                "status_name": row['status_name'],
                "memo": row['memo'],
                "create_by": row['create_by'],
                "create_date": row['create_date']
            }

            linenya = {
                "id": row['id'],
                "stock_code": row['stock_code'],
                "item_name": row['item_name'],
                "quantity": row['quantity'],
                "unit_price": row['unit_price']
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
