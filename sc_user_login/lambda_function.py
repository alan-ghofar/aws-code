import pymysql
import json
import datetime
import hashlib
import string
import random
import database_connection


def lambda_handler(event, context):
    # return send_response(event, 200)
    # TODO implement
    # test git dan upload lambda
    con = database_connection.database_connection_write()
    return functionLogin(con, event)


# function for response
def send_response(data, status_code):
    response = {
        "status": status_code,
        "data": data
    }
    return {
        'statusCode': status_code,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*'
            # 'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(response)
    }


# =============================== FUNCTION LOGIN
def functionLogin(con, event):
    cur = con.cursor()
    req = json.loads(event['body'])
    # req = event

    password = hashlib.md5(req['password'].encode('utf-8')).hexdigest()
    username = req['username']

    sql = "SELECT COUNT(nama_user), id FROM `sc_01_user` WHERE nama_user = %s AND password = %s AND delete_mark = 0 AND expired_date >= %s"
    cur.execute(sql, (username, password, datetime.datetime.today()))
    row = cur.fetchone()

    if row[0] == 0:
        data = {
            "message": "User tidak ditemukan!"
        }
        return send_response(data, 404)
    else:
        id_user = row[1]
        token_user = expiredToken(id_user, cur)

        con.commit()
        cur.close()

        data = {
            "message": "Login Success!",
            "token": token_user
        }
        return send_response(data, 200)


# =============================== FUNCTION GENERATE TOKEN
def generateToken(chars=string.ascii_uppercase + string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(50))


# =============================== FUNCTION CEK EXPIRED TOKEN
def expiredToken(id_user, cur):
    token = generateToken()

    sqlCek = "SELECT COUNT(id_user), token, id_user FROM `sc_01_token` WHERE id_user = %s AND expired_date >= CURDATE()"
    cur.execute(sqlCek, (id_user))
    rowCek = cur.fetchone()

    if rowCek[0] == 0:
        sqlDelete = "DELETE FROM `sc_01_token` WHERE id_user = %s"
        cur.execute(sqlDelete, (id_user))

        sqlInsert = "INSERT INTO `sc_01_token` (token, id_user, expired_date) VALUES (%s, %s, %s)"
        cur.execute(sqlInsert, (token, id_user, datetime.datetime.today()))

        token_user = token + "-" + str(id_user)

    else:
        token_user = rowCek[1] + "-" + str(id_user)

    return token_user

