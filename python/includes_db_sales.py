import pymysql.cursors


# GET VALUE DARI CONFIG SALES
def getValueConfigSales(name, con):
    cur = con.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT nilai FROM `sl_01_m_config` WHERE kode = %s"
    cur.execute(sql, (name))
    row = cur.fetchone()
    return row['nilai']
