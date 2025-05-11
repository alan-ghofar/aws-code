import boto3
import json
import pymysql

lambda_client = boto3.client('lambda', region_name='ap-southeast-3')


def lambda_handler(event, context):
    test_event = dict()

    response = lambda_client.invoke(
        FunctionName='arn:aws:lambda:ap-southeast-3:388158477574:function:db_connection',
        Payload=json.dumps(test_event),
    )

    data = response['Payload'].read().decode("utf-8")

    print(data)
    
    # xx = json.loads(data)
    # con = database_connection_write(xx)

    # cur = con.cursor()

    # sql = """
    # SELECT
    #     pl.id,
    #     pl.kode_perusahaan,
    #     p.nama_perusahaan,
    #     pl.kode_lokasi,
    #     l.nama_lokasi
    # FROM
    #     `st_02_perusahan_lokasi` pl
    #     LEFT JOIN `st_01_perusahaan` p ON ( p.kode_perusahaan = pl.kode_perusahaan )
    #     LEFT JOIN `st_01_lokasi` l ON ( l.kode_lokasi = pl.kode_lokasi )
    # WHERE
    #     pl.delete_mark = 0
    #     AND pl.active = 1
    # """

    # cur.execute(sql)
    # myresult = cur.fetchall()

    # allData = []
    # for row in myresult:
    #     data = {
    #         "id": row[0],
    #         "kode_perusahaan": row[1],
    #         "nama_perusahaan": row[2],
    #         "kode_lokasi": row[3],
    #         "nama_lokasi": row[4],
    #     }
    #     allData.append(data)

    # cur.close()
    # return send_response(allData, 200)


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


def database_connection_write(data):
    print("------- DB")
    print("endpoint: " + str(data['endpoint']))
    print("user: " + data['user'])
    print("password: " + data['password'])
    print("database: " + data['database'])
    print("------- DB")
    endpoint = str(data['endpoint'])
    usr = str(data['user'])
    pwd = str(data['password'])
    database = str(data['database'])

    # KONEKSI KE DB
    con = pymysql.connect(db=database, user=usr, passwd=pwd,
                          host=endpoint, port=3306, autocommit=True)

    return con
