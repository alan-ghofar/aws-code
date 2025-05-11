import tax_groups_db as tax_db

def getTaxForItems(con, taxTypeItems, prices, shippingCost, taxGroupId, taxIncluded, taxItemsArray={}):
    returnTaxArray = {}
    if(len(taxItemsArray)>0):
        returnTaxArray = taxItemsArray
    else:
        returnTaxArray = tax_db.getTaxGroupItemsAsArray(con, taxGroupId)
    
    #add index net 
    for index in returnTaxArray:
        returnTaxArray[index]['net'] = 0
        
    taxCategoryIncluded = tax_db.getTaxCategoryIncluded(con, taxIncluded)
    
    iterate = 0
    while iterate < len(taxTypeItems):
        itemTaxes = getTaxesForItem(con, taxTypeItems[iterate], returnTaxArray)
        
        if(itemTaxes is not None):
            taxMultiplier = 0
            for taxTypeId, taxItem in itemTaxes.items():
                taxRate = float(0 if(taxItem['rate'] is None) else taxItem['rate'])
                if(taxItem['code_tax_category'] in taxCategoryIncluded):
                    taxMultiplier += taxRate
            
            if(taxIncluded!=1):
                dpp = (float(prices[iterate]) * 100) / (100+taxMultiplier)
            else:
                #ini yg all exclude seharusnya
                dpp = float(prices[iterate])
            
            
            for taxTypeId, itemTax in itemTaxes.items():
                if(itemTax['rate'] is not None):
                    index = itemTax['tax_type_id']
                    
                    if(itemTax['code_tax_category'] in taxCategoryIncluded):
                        returnTaxArray[index]['dny'] = "A"
                        returnTaxArray[index]['value'] += (dpp*itemTax['rate'])/100
                        returnTaxArray[index]['net'] += dpp
                    else:
                        returnTaxArray[index]['dny'] = "B"
                        returnTaxArray[index]['value'] += (dpp*itemTax['rate'])/100
                        returnTaxArray[index]['net'] += dpp
                
        iterate+=1
        
    return returnTaxArray


def getTaxesForItem(con, taxTypeItemId, taxGroupItemsArray):
    #itemTaxType = tax_db.getItemTaxTypeForItem(con, stockId)
    itemTaxType = tax_db.getItemTaxTypeForItemById(con, taxTypeItemId)
    
    #if the item is exempt from all taxes then return 0
    if(itemTaxType['exempt']==1):
        return None
    
    #get the exemptions for this item tax type
    itemTaxTypeExemptionsDb = tax_db.getItemTaxTypeExemptions(con, itemTaxType["tax_type_item_id"]);
    
    #read them all into an array to minimize db querying
    itemTaxTypeExemptions = []
    for itemTaxTypeExempt in itemTaxTypeExemptionsDb:
        itemTaxTypeExemptions.append(itemTaxTypeExempt['tax_type_id'])
    
    returnTaxArray = {}
    for taxTypeId, taxGroupItem in taxGroupItemsArray.items():
        skip = False
        
        #if it's in the exemptions, skip
        for exemption in itemTaxTypeExemptions:
            if(taxTypeId==exemption):
                skip = True
                break
        
        if(skip==False):
            returnTaxArray[taxGroupItem['tax_type_id']] = taxGroupItem
    
    return returnTaxArray


#fungsi ini untuk ambil nilai price tanpa pajak (DPP)
def getTaxFreePriceForItem(con, taxTypeItemId, price, taxGroupId, taxIncluded, taxGroupArray={}):
    #if price is zero, then can't be taxed !
    if(price == 0):
        return 0
    
    if(taxIncluded == 1): # nilai 1 ini dari id table tax_included yaitu yang ALL TAX EXCLUDED
        #jika all tax exclude, langsung kembalikan price nya
        return price
    
    returnTaxArray = {}
    if(len(taxGroupArray)>0):
        returnTaxArray = taxGroupArray
    else:
        returnTaxArray = tax_db.getTaxGroupItemsAsArray(con, taxGroupId)
    
    itemTaxes = getTaxesForItem(con, taxTypeItemId, returnTaxArray)
    #if no exemptions or taxgroup is empty, then no included/excluded taxes
    if(itemTaxes is None):
        return price
    
    taxCategoryIncluded = tax_db.getTaxCategoryIncluded(con, taxIncluded)
    
    taxMultiplier = 0
    for taxTypeId, taxItem in itemTaxes.items():
        taxRate = float(0 if(taxItem['rate'] is None) else taxItem['rate'])
        if(taxItem['code_tax_category'] in taxCategoryIncluded):
            taxMultiplier += taxRate
    
    taxVal = 0
    for taxTypeId, taxItem in itemTaxes.items():
        taxRate = float(0 if(taxItem['rate'] is None) else taxItem['rate'])
        if(taxItem['code_tax_category'] in taxCategoryIncluded):
            taxVal += (price*taxRate)/(100+taxMultiplier)
    
    return price - taxVal


# fungsi untuk ambil nilai full price (price + tax)
def getFullPriceForItem(con, taxTypeItemId, price, taxGroupId, taxIncluded, taxGroupArray={}):
    # get dpp value
    dpp = getTaxFreePriceForItem(con, taxTypeItemId, price, taxGroupId, taxIncluded, taxGroupArray)
    
    # add dpp
    totalPrice = dpp
    
    # get tax 
    taxTypeItems = [taxTypeItemId]
    prices = [price]
    taxArrayReturn = getTaxForItems(con, taxTypeItems, prices, 0, taxGroupId, taxIncluded, taxGroupArray)
    
    for taxTypeId, taxItem in taxArrayReturn.items():
        # add tax value
        totalPrice += taxItem['value']
        
    return totalPrice