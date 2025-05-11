import json
import datetime
import pymysql.cursors
import valid_token as v_token
import valid_menu as v_menu
import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db
import dateutil.tz


# id menu cek table sc_02_menu
tableName = 'sl_05_m_cust_branch_salesman'


def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 404)

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
        	cbs.id,
        	cbs.branch_code,
        	branch.br_name,
        	cbs.customer_code,
        	cust.`name` cust_name,
        	cbs.salesman_id,
        	sales.salesman_name,
        	cbs.routes_id,
        	r.description 
        FROM
        	`sl_05_m_cust_branch_salesman` cbs
        	LEFT JOIN sl_03_m_customer_branch branch ON ( branch.branch_code = cbs.branch_code AND branch.customer_code = cbs.customer_code )
        	LEFT JOIN sl_02_m_customer cust ON ( cust.customer_code = cbs.customer_code )
        	LEFT JOIN sl_04_m_salesman sales ON ( sales.salesman_id = cbs.salesman_id )
        	LEFT JOIN sl_01_m_routes r ON ( r.id = cbs.routes_id ) 
        WHERE
            cbs.delete_mark = 0 
            AND cbs.active = 1
        """
        if params is not None:
            if params.get('id'):
                sql += " AND cbs.id = '" + params.get('id') + "'"
        
        sql += """
        ORDER BY
        	cbs.customer_code,
        	cbs.branch_code
        """

        cur.execute(sql)
        myresult = cur.fetchall()

        for row in myresult:
            data = {
                "id": row['id'],
                "customer_code": row['customer_code'],
                "cust_name": row['cust_name'],
                "branch_code": row['branch_code'],
                "br_name": row['br_name'],
                "salesman_id": row['salesman_id'],
                "salesman_name": row['salesman_name'],
                "routes_id": row['routes_id'],
                "description": row['description'],
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

    field = {
        "branch_code": req['branch_code'],
        "customer_code": req['customer_code'],
        "salesman_id": req['salesman_id'],
        "routes_id": req['routes_id'],
    }

    isDouble = inc_db.checkDoubleData(cur, field, tableName)

    if isDouble == "true":
        return inc_def.send_response_data("Data is exist", 500)

    try:
        sqlInsert = """
        INSERT INTO `sl_05_m_cust_branch_salesman` 
            ( 
                `branch_code`, `customer_code`, `salesman_id`, `routes_id`,
                active, create_by, create_date, delete_mark 
            ) 
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s, 0)
        """
        cur.execute(sqlInsert, (req['branch_code'], req['customer_code'], req['salesman_id'], req['routes_id'],
                                req['active'], vToken['id_user'], datetime.datetime.now(tz_JKT)))

        id = con.insert_id()
        inc_db.addAuditMaster(
            con, idMenu, inc_def.getActionCreate(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
        SELECT
            cbs.id,
            cbs.branch_code,
            cbs.customer_code,
            branch.br_name,
            cbs.salesman_id,
            sales.salesman_name,
            cbs.routes_id,
            r.description 
        FROM
            `sl_05_m_cust_branch_salesman` cbs
            LEFT JOIN sl_03_m_customer_branch branch ON ( branch.branch_code = cbs.branch_code AND branch.customer_code = cbs.customer_code )
            LEFT JOIN sl_04_m_salesman sales ON ( sales.salesman_id = cbs.salesman_id )
            LEFT JOIN sl_01_m_routes r ON ( r.id = cbs.routes_id ) 
        WHERE
            cbs.delete_mark = 0 
            AND cbs.active = 1
            AND cbs.id = %s
        """
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "id": row['id'],
        "branch_code": row['branch_code'],
        "customer_code": row['customer_code'],
        "br_name": row['br_name'],
        "salesman_id": row['salesman_id'],
        "salesman_name": row['salesman_name'],
        "routes_id": row['routes_id'],
        "description": row['description'],
    }

    cur.close()
    return inc_def.send_response_data(data, 200)


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
        UPDATE `sl_05_m_cust_branch_salesman` 
        SET
            `branch_code` = %s, 
            `customer_code` = %s, 
            `salesman_id` = %s, 
            `routes_id` = %s, 
            active = %s, 
            update_by = %s, 
            update_date = %s
        WHERE 
            `id` = %s
        """
        cur.execute(sqlUpdate, (
            req['branch_code'], req['customer_code'], req['salesman_id'], req['routes_id'],
            req['active'], vToken['id_user'], datetime.datetime.now(tz_JKT), id
        ))
        inc_db.addAuditMaster(
            con, idMenu, inc_def.getActionEdit(), id, '', tableName, vToken['id_user'])
    except Exception as e:
        con.rollback()
        con.close()
        return inc_def.send_response_data(str(e), 500)
    else:
        con.commit()

    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = """
        SELECT
            cbs.id,
            cbs.branch_code,
            cbs.customer_code,
            branch.br_name,
            cbs.salesman_id,
            sales.salesman_name,
            cbs.routes_id,
            r.description 
        FROM
            `sl_05_m_cust_branch_salesman` cbs
            LEFT JOIN sl_03_m_customer_branch branch ON ( branch.branch_code = cbs.branch_code AND branch.customer_code = cbs.customer_code )
            LEFT JOIN sl_04_m_salesman sales ON ( sales.salesman_id = cbs.salesman_id )
            LEFT JOIN sl_01_m_routes r ON ( r.id = cbs.routes_id ) 
        WHERE
            cbs.delete_mark = 0 
            AND cbs.active = 1
            AND cbs.id = %s
        """
    cur.execute(sql, (id))
    row = cur.fetchone()
    data = {
        "id": row['id'],
        "branch_code": row['branch_code'],
        "customer_code": row['customer_code'],
        "br_name": row['br_name'],
        "salesman_id": row['salesman_id'],
        "salesman_name": row['salesman_name'],
        "routes_id": row['routes_id'],
        "description": row['description'],
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
    UPDATE `sl_05_m_cust_branch_salesman` 
    SET 
        delete_by = %s, 
        delete_date = %s, 
        delete_mark = 1 
    WHERE 
        id = %s
    """

    cur.execute(sqlDelete, (vToken['id_user'], datetime.datetime.now(tz_JKT), id))
    inc_db.addAuditMaster(con, idMenu, inc_def.getActionDelete(),
                          id, '', tableName, vToken['id_user'])
    con.commit()

    data = {
        "message": "Deleted!"
    }

    cur.close()
    return inc_def.send_response_data(data, 200)
