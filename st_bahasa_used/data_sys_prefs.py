import string
import random

def getValueByName(name, cur) :
    sql = "SELECT nilai FROM `st_sys_prefs` WHERE kode = %s"
    cur.execute(sql, (name))
    row = cur.fetchone()
    return row[0]


def id_generator(chars=string.ascii_uppercase + string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(50))