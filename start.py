from datetime import datetime
from pprint import pprint

import firebase_admin
from firebase_admin import credentials, firestore
import overpy
from google.cloud.firestore_v1 import GeoPoint

print(datetime.now())


def createDbSet(osmNode):
    TYPES = {
        "fire_hydrant": "hydrant",
        "suction_point": "hydrant",
        "water_tank": "hydrant",
        "fire_water_pond": "hydrant",
        "defibrillator": "defibrillator",
        "phone": "phone"
    }

    ICONS = {
        "suction_point" : "suction",
        "water_tank": "water_tank",
        "fire_water_pond": "water_open",
        "defibrillator": "defibrillator",
        "phone": "phone"
    }

    ICONS_HYDRANT = {
        "pillar": "hydrant_piller",
        "pipe": "hydrant_unkown",
        "wall": "hydrant_unkown",
        "underground": "hydrant_under"
    }

    HYDRANT_POS = {
        "lane": "Fahrbahn",
        "parking_lot": "Parkbucht",
        "sidewalk": "Gehsteig",
        "green": "Wiese"
    }

    dbSet = {}
    dbSet[u"osmID"] = str(osmNode.id)
    dbSet[u"source"] = u"osm"
    dbSet[u"name"] = u""
    dbSet[u"pos"] = GeoPoint(osmNode.lat, osmNode.lon)
    try:
        dbSet[u"type"] = TYPES.get(osmNode.tags["emergency"])
    except KeyError:
        dbSet[u"type"] = ""
    try:
        if osmNode.tags["emergency"] == "fire_hydrant":
            dbSet[u"icon"] = ICONS_HYDRANT.get(osmNode.tags["fire_hydrant:type"])
        else:
            dbSet[u"icon"] = ICONS.get(osmNode.tags["emergency"])
    except KeyError:
        if osmNode.tags["emergency"] == "fire_hydrant":
            dbSet["icon"] = "hydrant_unkown"
        else:
            dbSet["icon"] = ""
    comment = ""
    for tag, val in osmNode.tags.items():
        if tag == "fire_hydrant:position":
            comment += u"Position: {} <br>".format(HYDRANT_POS.get(val))
        if tag == "name":
            comment += u"Addresse: {} <br>".format(val)
        if tag == "ref":
            comment += u"Referenz: {} <br>".format(val)
        if tag == "operator":
            comment += u"Betreiber: {} <br>".format(val)
        if tag == "fire_hydrant:diameter":
            comment += u"Durchmesser: {} <br>".format(val)
        if tag == "fire_hydrant:pressure":
            comment += u"Druck: {} <br>".format(val)
        if tag == "water_tank:volume":
            comment += u"Volumen: {} Liter<br>".format(val)
    if comment != "":
        dbSet[u"comment"] = comment[:-4].strip()
    else:
        dbSet[u"comment"] = u""
    return dbSet

CRED = credentials.Certificate("fb.creds")
APP = firebase_admin.initialize_app(CRED)
OSM = overpy.Overpass()
DB = firestore.client()
UPDATE_TIME = datetime.now()

fb = DB.collection(u'objects').where(u'source', u'==', u'osm').stream()
meta = DB.collection(u'objects').document(u'meta')
time = ""
try:
    time = meta.get().get('osmUpdate')
    time = time.replace(hour=0, minute=0, second=0, microsecond=0)
    time = time.isoformat('T')
except KeyError:
    print("Error: not osmUpdate key in meta data!")
    exit(255)
osm_result_update = OSM.query('[out:json];node["emergency"~"fire_hydrant|suction_point|water_tank|defibrillator|fire_water_pond|phone"](newer:"' + time + '")(49.1,9.9,49.4,10.6);out;') # (newer:"' + time + '")
osm_result_delete = OSM.query('[out:json];node["emergency"~"fire_hydrant|suction_point|water_tank|defibrillator|fire_water_pond|phone"](49.1,9.9,49.4,10.6);out;')

osmIDsDelete = []
osmIDsUpdate = []
dbIDs = []

toDelete = []

for osmID in osm_result_delete.get_node_ids():
    osmIDsDelete.append(str(osmID))

for osmID in osm_result_update.get_node_ids():
    osmIDsUpdate.append(str(osmID))

for doc in fb:
    dbIDs.append(doc.id)

for id in dbIDs:
    if id not in osmIDsDelete:
        toDelete.append(id)

toUpdate = []

for id in dbIDs:
    if id in osmIDsUpdate:
        toUpdate.append(id)
for id in osmIDsUpdate:
    if id not in dbIDs:
        toUpdate.append(id)

print("Elements to delete {}.".format(len(toDelete)))
print("Elements to update {}.".format(len(toUpdate)))

if len(toDelete) > 0:
    batch = DB.batch()
    count = 0
    for d in toDelete:
        count += 1
        if(count > 490):
            batch.commit()
            batch = DB.batch()
            count = 0
        ref = DB.collection(u'objects').document(d)
        batch.delete(ref)
    batch.commit()

if len(toUpdate) > 0:
    batch = DB.batch()
    count = 0
    for d in toUpdate:
        count += 1
        if(count > 490):
            batch.commit()
            batch = DB.batch()
            count = 0
        ref = DB.collection(u'objects').document(d)
        batch.set(ref, createDbSet(osm_result_update.get_node(int(d))))
    batch.commit()

meta.set({u"osmUpdate": UPDATE_TIME}, merge=True)

# doc_ref = db.collection(u'test')
# doc_ref.add({u'name': u'test', u'added': u'just now'})

# http://overpass-api.de/api/interpreter?data=%5Bout%3Ajson%5D%3Bnode%5B%22emergency%22%7E%22fire%5Fhydrant%7Csuction%5Fpoint%7Cwater%5Ftank%7Cdefibrillator%7Cfire%5Fwater%5Fpond%7Cphone%22%5D%2849%2E1%2C9%2E9%2C49%2E4%2C10%2E6%29%3Bout%3B%0A
