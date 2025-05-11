import json
import datetime
import pymysql.cursors
import valid_token
import valid_menu
import database_connection as db_con
import includes_definitions as inc_def

def lambda_handler(event, context):
    # return inc_def.send_response_data(event, 404)
    
    httpMethod = event['httpMethod']
    headers = event['headers']
    user_token = headers.get('token')
    #url_path = headers.get('path')

    if user_token is None:
        return inc_def.send_response_data("Access Denied", 403)
    else:
        vToken = valid_token.validationToken(user_token)
        if vToken == "false":
            return inc_def.send_response_data("Invalid Token", 404)
        else:
            # vMenu = valid_menu.validationMenu(vToken['id_user'], url_path)
            # if vMenu == "false":
            #     return inc_def.send_response_data("Forbidden access for this menu", 403)
            event['data_user'] = vToken

    return getHttpMethod(httpMethod, event)


def getHttpMethod(httpMethod, event):
    if httpMethod == 'GET':
        con = db_con.database_connection_read()
        return functionGet(con, event)
    else:
        return inc_def.send_response_data([], 404)


# =============================== FUNCTION GET
def functionGet(con, event):
    cur = con.cursor(pymysql.cursors.DictCursor)
    vToken = event['data_user']
    params = event['queryStringParameters']
    
    allMenu = getDataAllMenuAllow(cur, vToken['id_user'])
    
    sqlCategory = """
    SELECT
    	mc.nama_category,
    	mc.keterangan,
    	m.id_menu_category 
    FROM
    	`sc_02_user_resp` ur
    	LEFT JOIN `sc_04_responsibility_item` ri ON ( ri.id_resp = ur.id_responsibility )
    	LEFT JOIN `sc_02_menu` m ON ( m.id = ri.id_menu )
    	LEFT JOIN `sc_01_menu_category` mc ON ( mc.id = m.id_menu_category ) 
    WHERE
    	ur.id_user = %s
    """
    if params is not None:
        if params.get('id_perusahaan'):
            sqlCategory += " AND ur.kode_perusahaan = '" + params.get('id_perusahaan') + "'"
            
    sqlCategory += """
    	AND ur.delete_mark = 0 
    	AND ri.delete_mark = 0 
    	AND m.delete_mark = 0 
    	AND mc.delete_mark = 0 
    	AND ri.id IS NOT NULL 
    GROUP BY
    	m.id_menu_category
    """

    cur.execute(sqlCategory, (vToken['id_user']))
    resCtgMenu = cur.fetchall()
    
    dataAllMenu = []
    dataBreadCrump = dataBreadCrumpSubMenu = []
    for rowCtg in resCtgMenu:
        # sql mencari sub menu Transaction, Inquiry, Maintenance
        sqlHeader = """
        SELECT
            id,
            nama_menu,
            path,
            keterangan,
            id_menu_category 
        FROM
            sc_02_menu 
        WHERE
            parent_id = 0
            AND delete_mark = 0
            AND active = 1
            AND id_menu_category = %s
        ORDER BY
            priority ASC
        """
        cur.execute(sqlHeader, (rowCtg['id_menu_category']))
        resHead = cur.fetchall()
        
        dataParent = []
        for rowHead in resHead:
            if rowHead['path'] not in allMenu:
                continue
            
            # sql untuk memasukkan menu ke sub menu
            sqlMenu = """
            SELECT
                m.nama_menu,
                m.path,
                m.keterangan,
                m.id_menu_category 
            FROM
                `sc_02_user_resp` ur
                LEFT JOIN `sc_04_responsibility_item` ri ON ( ri.id_resp = ur.id_responsibility )
                LEFT JOIN `sc_02_menu` m ON ( m.id = ri.id_menu ) 
            WHERE
                ur.id_user = %s
                AND m.id_menu_category = %s 
                AND m.parent_id = %s
                AND ur.delete_mark = 0 
                AND ri.delete_mark = 0 
                AND m.delete_mark = 0 
            GROUP BY
                m.path
            ORDER BY
                m.priority ASC
            """
            cur.execute(sqlMenu, (vToken['id_user'], rowCtg['id_menu_category'], rowHead['id']))
            resMenu = cur.fetchall()
            
            dataMenu = []
            for rowMenu in resMenu:
                breadCrumpCtg = {
                    "url": "",
                    "menu": rowCtg["nama_category"]
                }
                dataBreadCrump.append(breadCrumpCtg)
                
                breadcrumpSubMenu = {
                    "url": "",
                    "menu": rowHead["keterangan"]
                }
                dataBreadCrump.append(breadcrumpSubMenu)
            
                breadCrumpMenu = {
                    "url": "/" + rowMenu["path"] + "/index",
                    "menu": rowMenu["keterangan"]
                }
                dataBreadCrump.append(breadCrumpMenu)
                menu = {
                    "menu": rowMenu["path"],
                    "url": "/" + rowMenu["path"] + "/index",
                    "title": rowMenu["keterangan"],
                    "breadcrump": dataBreadCrump
                }
                dataMenu.append(menu)
                dataBreadCrump = []
            
            dataBreadCrumpSubMenu = []
            breadCrumpCtg = {
                "url": "",
                "menu": rowCtg["nama_category"]
            }
            dataBreadCrumpSubMenu.append(breadCrumpCtg)
            
            breadcrumpSubMenu = {
                "url": "/" + rowHead["path"] + "/index",
                "menu": rowHead["keterangan"]
            }
            dataBreadCrumpSubMenu.append(breadcrumpSubMenu)
            
            parent = {
                "menu": rowHead["path"],
                "url": None if dataMenu else "/" + rowHead["path"] + "/index",
                "title": rowHead["keterangan"],
                "breadcrump": None if dataMenu else dataBreadCrumpSubMenu,
                "child": None if not dataMenu else dataMenu
            }
            
            dataParent.append(parent)
            
    
        listCtg = {
            "menu": rowCtg["nama_category"],
            "url": None,
            "title": rowCtg["keterangan"],
            "child": dataParent
            
        }
        dataAllMenu.append(listCtg)
    
    return inc_def.send_response_data(dataAllMenu, 200)


def getDataAllMenuAllow(cur, id_user):
    sql = """
    SELECT
    	ri.id_resp,
    	m.nama_menu,
    	m.path,
    	a.nama_action,
    	ma.params 
    FROM
    	sc_02_user_resp ur
    	LEFT JOIN sc_04_responsibility_item ri ON ( ri.id_resp = ur.id_responsibility )
    	LEFT JOIN sc_03_menu_action ma ON ( ma.id = ri.id_menu_action )
    	LEFT JOIN sc_02_menu m ON ( m.id = ma.id_menu )
    	LEFT JOIN sc_01_action a ON ( a.id = ma.id_action ) 
    WHERE
    	ur.id_user = %s 
    	AND ur.expired_date > CURDATE() 
    	AND ri.id IS NOT NULL 
    	AND ur.delete_mark = 0 
    	AND ri.delete_mark = 0 
    	AND m.delete_mark = 0 
    	AND ma.delete_mark = 0 
    	AND a.delete_mark = 0 
    	AND ri.active = 1 
    	AND m.active = 1 
    	AND ma.active = 1 
    	AND a.active = 1 
    	AND a.id = 6
    """

    cur.execute(sql, (id_user))
    resAllMenu = cur.fetchall()
    allMenu = []
    for row in resAllMenu:
        menu = row['path']
        allMenu.append(menu)
        
        
    sqlSub = """
    SELECT
        sub.keterangan,
    	sub.path
    FROM
    	sc_02_menu m
    	LEFT JOIN sc_02_menu sub ON ( sub.id = m.parent_id ) 
    WHERE
    	m.parent_id != 0 
    	AND m.delete_mark = 0 
    	AND m.active = 1 
    	AND sub.delete_mark = 0 
    	AND sub.active = 1 
    GROUP BY
    	sub.id
    """

    cur.execute(sqlSub)
    resAllSub = cur.fetchall()
    for row in resAllSub:
        menu = row['path']
        allMenu.append(menu)

    return allMenu