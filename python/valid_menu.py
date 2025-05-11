import database_connection


def validationMenu(id_user, path_menu):
    con = database_connection.database_connection_read()

    allMenu = getAllMenuAllow(con, id_user)

    if path_menu not in allMenu:
        return "false"
    else:
        return "true"


def getAllMenuAllow(con, id_user):
    cur = con.cursor()
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
    """

    cur.execute(sql, (id_user))
    resAllMenu = cur.fetchall()
    allMenu = []
    for row in resAllMenu:
        menu = row[2] + "/" + row[3] if row[4] is None else row[2] + \
            "/" + row[3] + "/:" + row[4]
        allMenu.append(menu)

    cur.close()
    return allMenu
