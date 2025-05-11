import database_connection
from datetime import datetime


def validationToken(token_user):
    con = database_connection.database_connection_read()

    cur = con.cursor()
    sp = token_user.split("-")

    token = sp[0]
    id_user = sp[1]

    sql = "SELECT COUNT(token) FROM `sc_01_token` WHERE token = %s AND id_user = %s AND expired_date >= %s"
    cur.execute(sql, (token, id_user, datetime.today().strftime('%Y-%m-%d')))
    row = cur.fetchone()

    cur.close()
    if row[0] == 0:
        return "false"
    else:
        return {
            "id_user": id_user
        }
