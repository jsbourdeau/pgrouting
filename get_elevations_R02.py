import urllib.request
import json
import pandas as pd
import time
from sqlalchemy import create_engine

# il faut modifier la ligne suivante avec les informations de connexion (user, password et dbname) à PostgreSQL (comme dans QGIS) :
engine = create_engine('postgresql://user:password@localhost:5432/dbname', echo=False)

def create_pandas_table(sql_query, database = engine):
    with database.connect() as con:    
        table = pd.read_sql_query(sql_query, con)
    return table

#ici il faut specifier le nom du schema et de la table osm
schema = 'public'
table_name = 'hh_2po_4pgr'

command = """ALTER TABLE {0}.{1} ADD COLUMN IF NOT EXISTS elevation_debut numeric""".format(schema, table_name)
with engine.connect() as con:
    con.execute(command) 

command = """ALTER TABLE {0}.{1} ADD COLUMN IF NOT EXISTS elevation_fin numeric""".format(schema, table_name)
with engine.connect() as con:
    con.execute(command)

#ici il faut specifier le nom de la colonne de geometrie
geometry_column = 'geom_way'

command = """SELECT *, ST_X(ST_StartPoint({0})) as lon_debut,  ST_Y(ST_StartPoint({0})) as lat_debut,
ST_X(ST_EndPoint({0})) as lon_fin,  ST_Y(ST_EndPoint({0})) as lat_fin  FROM  {1}.{2} WHERE elevation_debut IS NULL OR elevation_fin IS NULL;""".format(geometry_column, schema, table_name)

with engine.connect() as con:    
    osm_network = pd.read_sql_query(command, con)
#osm_network = create_pandas_table(command,)
#osm_network = osm_network.to_records()

for ix, link in osm_network.iterrows():
    print(ix) 

    done = 0

    while done == 0:

        try:

            link_id = link['id']
            lat_debut = link['lat_debut']
            lon_debut = link['lon_debut']    
    
            lat_fin = link['lat_fin']
            lon_fin = link['lon_fin']    
    
    
            #ici on va chercher l'elevation du debut
            url_debut = 'http://geogratis.gc.ca/services/elevation/cdem/altitude?lat=' + str(lat_debut) + '&lon=' + str(lon_debut) 
    
   
            information_req = urllib.request.urlopen(url_debut)
            information = information_req.read()
            test = json.loads(information)
    
            elevation_debut = test['altitude'] #(test.split(start))[1].split(end)[0]
    
            #ici on va chercher l'elevation de la fin
            url_fin = 'http://geogratis.gc.ca/services/elevation/cdem/altitude?lat=' + str(lat_fin) + '&lon=' + str(lon_fin) 
    
            information_req = urllib.request.urlopen(url_fin)
            information = information_req.read()
            test = json.loads(information)
            elevation_fin = test['altitude'] #(test.split(start))[1].split(end)[0]
    
            if str(elevation_debut).isnumeric() and str(elevation_fin).isnumeric():
                command = """UPDATE  {0}.{1} SET elevation_debut = {2}, elevation_fin = {3} WHERE id = {4};""".format(schema, table_name, elevation_debut, elevation_fin, link_id)
                with engine.connect() as con:
                    con.execute(command)
    
            done = 1
            time.sleep(0.5)

        except:
            # print 'dodo' 
            time.sleep(60)
            done = 0
