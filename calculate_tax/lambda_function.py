import json

import database_connection as db_con
import includes_definitions as inc_def
import includes_db_general as inc_db
import tax_calc

def lambda_handler(event, context):
    req = json.loads(event['body'])
    con = db_con.database_connection_read()
    # contoh ambil hitungan tax
    taxGroupId = req['tax_group_id']
    taxIncluded = req['tax_included']
    taxTypeItems = []
    prices = []
    for linedata in req['line_items']:
        taxTypeItems.append(linedata['tax_type_item_id'])
        prices.append(linedata['price'])
    
    taxItemsArray = {}
    taxArrayReturn = tax_calc.getTaxForItems(con, taxTypeItems, prices, 0, taxGroupId, taxIncluded, taxItemsArray)
    return inc_def.send_response_data(taxArrayReturn, 200)
    
    #contoh ambil dpp
    # taxTypeItemId = 5
    # price = 13675000
    # taxGroupId = 5
    # taxIncluded = 3
    # taxItemsArray = {}
    # priceDpp = tax_calc.getTaxFreePriceForItem(con, taxTypeItemId, price, taxGroupId, taxIncluded, taxItemsArray)
    # return inc_def.send_response_data(priceDpp, 200)
    
    #contoh ambil price full (price + tax)
    # taxTypeItemId = 5
    # price = 273500 #13675000 #820500.00
    # taxGroupId = 5
    # taxIncluded = 3
    # taxItemsArray = {}
    # fullPrice = tax_calc.getFullPriceForItem(con, taxTypeItemId, price, taxGroupId, taxIncluded, taxItemsArray)
    # return inc_def.send_response_data(fullPrice, 200)