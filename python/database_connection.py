import pymysql


def database_connection_read():
    endpoint = "dberpmodular.cluster-ro-cypstyv3silb.ap-southeast-1.rds.amazonaws.com"
    usr = "permen"
    pwd = "Legi#seru321&"
    database = "dbmodularerp"

    # KONEKSI KE DB
    con = pymysql.connect(db=database, user=usr, passwd=pwd,
                          host=endpoint, port=3306, autocommit=True)

    return con


def database_connection_write():
    endpoint = "dberpmodular.cluster-cypstyv3silb.ap-southeast-1.rds.amazonaws.com"
    usr = "permen"
    pwd = "Legi#seru321&"
    database = "dbmodularerp"

    # KONEKSI KE DB
    con = pymysql.connect(db=database, user=usr, passwd=pwd,
                          host=endpoint, port=3306, autocommit=True)

    return con
