import lxml.html
import scraperwiki
import requests
import time
import sys
import os
from operator import itemgetter
from itertools import product, imap as map
from datetime import date


# these fields don't contain any data
# too many fields also cause a SqliteError: sqliteexecute:
# sqlite3.Error: too many SQL variables This seems to occur when ever
# 1000 columns become used
def removeBogusColumnsFromDict(d):
    #delete all keys that start with these patterns
    keyPatternsToDelete = ["Add_Another_", "Add_Associated_", "ai",
                           "Color", "Levels", "LinesCount",
                           "NumberOf", "Num_Levels", "Opacity",
                           "Overlay_", "Zoom_Factor", "Weight"]
    for key in d.keys():
        if any(key.startswith(pat) for pat in keyPatternsToDelete):
            del d[key]

    return d

def checkForValue(value):
        if len(value) > 0:
                return unicode(value[0])
        else:
                return ""

#remove problem characters that don't work as sql column names
def makeNiceKey(value):
        value = value \
                .replace('(', '') \
                .replace(')', '') \
                .replace('%', 'Percent') \
                .replace('-', '_') \
                .replace('/', '__') \
                .replace('###', '___') \
                .replace(':_', '__') \
                .replace('         ', '') \
                .replace('&#9;', '') \
                .replace('\t', '') \
                .replace('enumfield', '')
        while value.find('__') > 0:
            value = value.replace('__', '_')
        return value

scraperwiki.sqlite.execute(
    u"CREATE TABLE IF NOT EXISTS powerplants"
    u"(GEO_Assigned_Identification_Number TEXT,"
    u" Name TEXT, Type TEXT, Country TEXT, State TEXT,"
    u" Type_of_Plant_rng1 TEXT, Type_of_Fuel_rng1_Primary TEXT,"
    u" Type_of_Fuel_rng2_Secondary TEXT,"
    u" Design_Capacity_MWe_nbr NUMBER,"
    u" Date_of_Scraping TEXT)"
)

# figure out what's already been downloaded
recentlyupdatedIDs = frozenset(
    map(itemgetter('geoid'),
        scraperwiki.sqlite.select("`GEO_Assigned_Identification_Number` AS geoid"
                                  " FROM powerplants"
                                  " WHERE `Date_of_Scraping` >= date('now','-1 month')"))
)

updatedIDs = set()

fuelTypes = ["Coal", "Gas", "Oil", "Hydro", "Geothermal", "Nuclear",
             "Solar_PV", "Solar_Thermal", "Waste", "Wind"]

for fuelType in fuelTypes:
    fuelTypeURL = "http://globalenergyobservatory.org/list.php?db=PowerPlants&type=" + fuelType
    print fuelTypeURL
    root = lxml.html.fromstring(requests.get(fuelTypeURL).text)
    links = root.xpath("//tr[@class='odd_perf' or @class='even_perf']/td[1]/a/@href")

    for link in links:
        plantID = link.replace("geoid/", "")
        plantURL = "http://globalenergyobservatory.org/" + link

        if plantID in recentlyupdatedIDs: continue
        print plantURL

        try:
            html = requests.get(plantURL).text
            root = lxml.html.fromstring(html)
        except:
            print "Error downloading " + plantURL

        installationInfo = dict()
        unitList = list()
        geoid = -1

        inputFields = root.xpath("//input | //select")
        for inputField in inputFields:

            #TODO how to deal with checkboxes?
            typeVal = checkForValue(inputField.xpath("./@type"))
            idVal = checkForValue(inputField.xpath("./@id"))
            classVal = checkForValue(inputField.xpath("./@class"))
            valueVal = checkForValue(inputField.xpath("./@value"))
            nameVal = checkForValue(inputField.xpath("./@name"))
            chkVal = checkForValue(inputField.xpath("./@checked"))
            catVal = checkForValue(inputField.xpath("ancestor::div[h1]/@id"))
            selOptVal = checkForValue(inputField.xpath("./option[@selected]/@value"))

            if (((typeVal == "text" or typeVal == "hidden") or
                 ((typeVal == "radio" or typeVal == "checkbox") and len(chkVal) > 0))
                and len(valueVal) > 0 and len(idVal) > 0):
                if idVal == "GEO_Assigned_Identification_Number":
                    geoid = valueVal
                if catVal <> 'UnitDescription_Block':
                    installationInfo[makeNiceKey(idVal)] = valueVal
                else:
                    # use different table for unit details to avoid
                    # having too many fields in main table
                    parts = makeNiceKey(idVal).rpartition('_')
                    while int(parts[2]) > len(unitList):
                        unitList.append({'GEO_Assigned_Identification_Number': geoid,
                                         'Unit_Nbr': len(unitList)+1})
                    unitList[int(parts[2])-1][parts[0]] = valueVal
            elif len(selOptVal) > 0 and not(selOptVal.startswith("Please Select")) and len(idVal) > 0:
                installationInfo[makeNiceKey(idVal)] = selOptVal

        #add in fuel type
        #installationInfo["Fuel_type"] = fuelType

        installationInfo = removeBogusColumnsFromDict(installationInfo)

        # TODO Description_ID does not always correspond to the actual
        # page URL, need to contact Rajan Gupta of GEO as he's
        # probably not aware of this.
        # select Description_ID, CurrentPage_sys, GEO_Assigned_Identification_Number from `swdata` where Description_ID != GEO_Assigned_Identification_Number OR replace(CurrentPage_sys, "/geoid/", "") != GEO_Assigned_Identification_Number
        # Using GEO_Assigned_Identification_Number, it corresponds to CurrentPage_sys except in one case

        installationInfo['Date_of_Scraping'] = date.today()

        try:
            assert 'GEO_Assigned_Identification_Number' in installationInfo, \
                "No GEO_Assigned_Identification_Number given"

            #primary key is based on id
            scraperwiki.sqlite.save(unique_keys=["GEO_Assigned_Identification_Number"],
                                    data=installationInfo, table_name="powerplants")
            if len(unitList) > 0:
                scraperwiki.sqlite.save(
                    unique_keys=["GEO_Assigned_Identification_Number", "Unit_Nbr"],
                    data=unitList, table_name="ppl_units"
                )

            updatedIDs.add(plantID)
        except:
            print "Error saving to DB" + ": " + str(sys.exc_info()[1])

        time.sleep(2) #sleep a little to be kind to the server, running into "Temporary failure in name resolution"

# Remove old cruft, i.e. everything that has not been updated although
# it was scheduled to

allIDs = set(map(itemgetter('geoid'),
                 scraperwiki.sqlite.select("`GEO_Assigned_Identification_Number` AS geoid"
                                           " FROM powerplants")))
oldIDs = allIDs - (updatedIDs | recentlyupdatedIDs)

if len(oldIDs):
    for table, i in product(['powerplants', 'ppl_units'], oldIDs):
        scraperwiki.sqlite.execute('DELETE FROM %s WHERE `GEO_Assigned_Identification_Number`="%s"' % (table, i))
    scraperwiki.sqlite.commit()

    print "Removed {} old entries from database".format(len(oldIDs))
