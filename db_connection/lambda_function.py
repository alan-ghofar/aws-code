import json

def lambda_handler(event, context):
    # return database_connection()
    return {
        "endpoint": "lab-modular-new.cluster-ci1oog0ibuet.ap-southeast-3.rds.amazonaws.com",
        "user": "lab_modular",
        "password": "saparirecordlhoiki2348901",
        "database": "labmodular"
        # "endpoint": "127.0.0.1",
        # "user": "root",
        # "password": "",
        # "database": "labmodular"
    }
