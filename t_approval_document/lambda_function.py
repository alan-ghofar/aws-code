import json
import datetime
import valid_token
import valid_menu
import pymysql.cursors
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db
#import hooks

tableName = 'st_03_document_workflow_log'

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
    else:
        return inc_def.send_response_data([], 404)
    
    idMenu = inc_db.getIdMenuByTableName(con, tableName)
    dataMenu = inc_db.getDataMenu(con, idMenu)
    pathMenunya = dataMenu['path'] #inc_db.getPathMenu(con, idMenu)
    
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
    if httpMethod == 'POST':
        return functionPost(con, event)
    elif httpMethod == 'GET':
        return functionGet(con, event)
    else:
        return inc_def.send_response_data([], 404)

# =============================== FUNCTION GET
def functionGet(con, event):
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)
    vToken = event['data_user']
    params = event['queryStringParameters']
    headers = event['headers']
    kode_perusahaan = headers.get('kode_perusahaan')
    idUser = vToken['id_user']
    filterTypeDoc = '%'
    filterTransNo = '%'
    filterReference = '%'
    
    #status action
    #0 = open , 1 = pending, 2 = approve, 3 = reject, 4 = denied, 5 = approval by pass
    sql = """
    SELECT
    	wflog.id,
    	wflog.type_doc,
    	wflog.trans_no,
        ref.refs,
    	wflog.urutan,
        wflog.nilai,
        wflog.kode_perusahaan,
        wflog.kode_lokasi,
        wflog.type_approval,
        wflog.id_user,
        wflog.id_responsibility,
        wflog.create_by,
        wflog.create_date
    FROM
        st_03_document_workflow_log wflog 
        left join st_02_refs ref
        on (wflog.type_doc = ref.type_doc
		and wflog.trans_no = ref.trans_no)
    WHERE
        wflog.delete_mark = 0 
        AND wflog.status_action = 0 
        AND  (wflog.id_user = %s OR wflog.id_responsibility 
                IN (select uresp.id_responsibility 
                from sc_02_user_resp uresp 
                where uresp.id_user = %s 
                and delete_mark = 0 
                and kode_perusahaan = wflog.kode_perusahaan))
        AND (wflog.kode_lokasi IS NULL OR  wflog.kode_lokasi 
            IN (select sul.kode_lokasi 
            from sc_02_user_lokasi sul 
            where sul.delete_mark = 0 
            and sul.active = 1 
            and sul.id_user = %s 
            and sul.kode_perusahaan = wflog.kode_perusahaan))
        AND wflog.type_doc LIKE %s 
        AND wflog.trans_no LIKE %s
        AND ref.refs LIKE %s
        AND wflog.kode_perusahaan LIKE %s
    """ 
    if params is not None:
        if params.get('trans_no') is not None:
            filterTransNo = params.get('trans_no')
        if params.get('type_doc') is not None:
            filterTypeDoc = params.get('type_doc')
        if params.get('reference') is not None:
            filterReference = "%"+params.get('reference')+"%"
    
    cur.execute(sql, (idUser, idUser, idUser, filterTypeDoc, filterTransNo, filterReference, kode_perusahaan))
    myresult = cur.fetchall()
    returnData = []
    lineItemData = []
    headerData = []
    temptrans_no=0
    for row in myresult:
        headerData = {
            "id": row['id'],
            "type_doc": row['type_doc'],
            "trans_no": row['trans_no'],
            "reference": row['refs'],
            "urutan": row['urutan'],
            "nilai": row['nilai'],
            "kode_perusahaan": row['kode_perusahaan'],
            "kode_lokasi": row['kode_lokasi'],
            "type_approval": row['type_approval'],
            "id_user": row['id_user'],
            "id_responsibility": row['id_responsibility'],
            "create_by": row['create_by'],
            "create_date": row['create_date']
        }
        returnData.append(headerData)
            
    # except Exception as e:
    #     return inc_def.send_response_data(str(e), 500)
    
    cur.close()
    return inc_def.send_response_data(returnData, 200)
    
# =============================== FUNCTION POST
def functionPost(con, event):
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)
    req = json.loads(event['body'])
    vToken = event['data_user']
    headers = event['headers']
    inKodePerusahaan = headers.get('kode_perusahaan')
    inTypeDocument = req['type_doc']
    inTransNo = req['trans_no']
    inStatusAction = req['status_action']
    inDocumentWorkflowLog = req['id_document_workflow_log']
    inMemo = req['memo']

    stritm = ''
    if req.get('line_items') :
        for row in req['line_items']:
            linenya = str(row['id_line_detail']) + "=-="
            linenya += row['stock_code'] + "=-="
            linenya += str(row['quantity'])
            if(stritm==''):
                stritm = linenya
            else:
                stritm = stritm + "=+=" + linenya
    
    try:
        sqlTrans = """
        CALL `document_approval_transaction` (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sqlTrans, (idMenu, inKodePerusahaan, inDocumentWorkflowLog, inTypeDocument, inTransNo, 
        inStatusAction, 0, str(vToken['id_user']), stritm, inMemo))
        
        rowTrans = cur.fetchone()
        if rowTrans['status'] == 200:
            statusDocument = rowTrans['get_status_code_document']
        else:
            con.rollback()
            con.close
            return inc_def.send_response_data(rowTrans['message'], 400)
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e) , 500)
    else:
        con.commit()
        data = {
            "trans_no":inTransNo,
            "status_code": statusDocument
        }
        cur.close()
        return inc_def.send_response_data(data, 200)


