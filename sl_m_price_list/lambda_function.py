import json
import datetime
import pymysql.cursors
import valid_token as v_token
import valid_menu as v_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db

# id menu cek table sc_02_menu
tableName = 'sl_04_m_price_list'


def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 404)

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
        vToken = v_token.validationToken(user_token)
        if vToken == "false":
            return inc_def.send_response_data("Invalid Token", 404)
        else:
            vMenu = v_menu.validationMenu(vToken['id_user'], url_path)
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

    allData = []
    try:
        sql = """
        SELECT
            pl.id,
            pl.id_level_pricelist,
            pl.gpl_id,
            gpl.description gpl_name,
            pl.item_code,
            ic.description item_name,
            pl.sales_type_id,
            st.sales_type,
            pl.price,
            pl.start_date,
            pl.end_date 
        FROM
            `sl_04_m_price_list` pl
            LEFT JOIN `sl_02_m_grouping_price_list` gpl ON ( gpl.id = pl.gpl_id )
            LEFT JOIN `in_01_m_item_codes` ic ON ( ic.item_code = pl.item_code )
            LEFT JOIN `sl_01_m_sales_type` st ON ( st.id = pl.sales_type_id ) 
        WHERE
            pl.delete_mark = 0 
            AND pl.active = 1 
            AND ic.delete_mark = 0 
            AND ic.active = 1 
            AND st.delete_mark = 0 
            AND st.active = 1
        """
        if params is not None:
            if params.get('id'):
                sql += " AND pl.id = '" + params.get('id') + "'"

        cur.execute(sql)
        myresult = cur.fetchall()

        for row in myresult:
            data = {
                "id": row['id'],
                "id_level_pricelist": row['id_level_pricelist'],
                "gpl_id": row['gpl_id'],
                "gpl_name": row['gpl_name'],
                "item_code": row['item_code'],
                "item_name": row['item_name'],
                "sales_type_id": row['sales_type_id'],
                "sales_type": row['sales_type'],
                "price": row['price'],
                "start_date": row['start_date'],
                "end_date": row['end_date'],
            }
            allData.append(data)
    except Exception as e:
        con.close()
        return inc_def.send_response_data(str(e), 500)

    cur.close()
    return inc_def.send_response_data(allData, 200)


# =============================== FUNCTION POST
def functionPost(con, event):
    con.begin()
    cur = con.cursor()
    req = json.loads(event['body'])
    vToken = event['data_user']

    try:
        field = {
            "id_level_pricelist": req['id_level_pricelist'],
            "gpl_id": req['gpl_id'],
            "item_code": req['item_code'],
            "sales_type_id": req['sales_type_id'],
        }
        
        isDouble = checkDoubleDataPriceList(cur, field, tableName)
        
        if isDouble != "false" :
            isUpdate = functionUpdateData(con, req, vToken['id_user'], isDouble)
            if isUpdate != 200:
                con.rollback()
                con.close()
                return inc_def.send_response_data(isUpdate, 500)
        
        sqlInsert = """
        INSERT INTO `sl_04_m_price_list` 
            ( 
                id_level_pricelist, gpl_id, item_code, sales_type_id,
                price, start_date, end_date,
                active, create_by, create_date, delete_mark 
            ) 
        VALUES 
            (
                %s, %s, %s, %s, 
                %s, %s, %s, 
                %s, %s, %s, 0
            )
        """
        cur.execute(sqlInsert, (
            req['id_level_pricelist'], req['gpl_id'], req['item_code'], req['sales_type_id'],
            req['price'], req['start_date'], req['end_date'], 
            req['active'], vToken['id_user'], datetime.date.today())
        )
        id = con.insert_id()

        
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur.close()
    return inc_def.send_response_data("Inserted!", 200)


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
        UPDATE `sl_04_m_price_list` 
        SET
            id_level_pricelist = %s, 
            gpl_id = %s, 
            item_code = %s, 
            sales_type_id = %s, 
            price = %s, 
            start_date = %s, 
            end_date = %s, 
            `active` = %s, 
            update_by = %s, 
            update_date = %s
        WHERE 
            `id` = %s
        """
        cur.execute(sqlUpdate, (
            req['id_level_pricelist'], req['gpl_id'], req['item_code'], req['sales_type_id'],
            req['price'], req['start_date'], req['end_date'], 
            req['active'], vToken['id_user'], datetime.date.today(), id
        ))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur.close()
    return inc_def.send_response_data("Updated!", 200)


# =============================== FUNCTION DELETE
def functionDelete(con, event):
    cur = con.cursor()
    vToken = event['data_user']
    params = event['queryStringParameters']
    id = params['id']
    sqlDelete = """
    UPDATE `sl_04_m_price_list` 
    SET 
        delete_by = %s, 
        delete_date = %s, 
        delete_mark = 1 
    WHERE 
        class_id = %s
    """

    cur.execute(sqlDelete, (vToken['id_user'], datetime.date.today(), id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(), id, '', tableName, vToken['id_user'])
    con.commit()

    cur.close()
    return inc_def.send_response_data("Deleted!", 200)


# =============================== FUNCTION UPDATE JIKA ADA DATA YG SAMA
def functionUpdateData(con, fields, id_user, id):
    con.begin()
    cur = con.cursor(pymysql.cursors.DictCursor)

    start_date = datetime.datetime.strptime(fields['start_date'], '%Y-%m-%d')

    try:
        sqlUpdate = """
        UPDATE `sl_04_m_price_list` 
        SET
            end_date = %s, 
            `active` = %s, 
            update_by = %s, 
            update_date = %s
        WHERE 
            `id` = %s
        """
        cur.execute(sqlUpdate, (
            start_date - datetime.timedelta(days=1),
            0, id_user, datetime.date.today(), id
        ))
        inc_db.addAuditMaster(con, idMenu, inc_def.getActionEdit(), id, '', tableName, id_user)
    except Exception as e:
        return str(e)

    return 200

def checkDoubleDataPriceList(cur, field, table):
    sql = "SELECT IFNULL(MAX(id), 0) FROM " + table + " WHERE delete_mark = 0 AND active = 1"

    for column, value in field.items():
        sql += " AND `" + column + "` = '" + str(value) + "'"
        

    cur.execute(sql)
    row = cur.fetchone()
    
    if row[0] == 0:
        return "false"
    
    return row[0]
