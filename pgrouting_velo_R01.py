from sqlalchemy import create_engine
import datetime
import pandas as pd

now_1 = datetime.datetime.now()

# il faut modifier la ligne suivante avec les informations de connexìon à PostgreSQL (comme dans QGIS) :
engine = create_engine('postgresql://user:password@localhost:5432/dbname', echo=False)

# il faut modifier le lien vers le fichier :
paires_od = pd.read_csv('/Users/jeansimonbourdeau/Desktop/pgrouting/defi_velo_mrv_orig_dest.csv', sep = ';')
#paires_od_records = paires_od.to_records()

command = """DROP TABLE IF EXISTS od_chemins;
CREATE TABLE od_chemins (geom_chemin geometry, seq_liens_osm varchar, seq_liens varchar,  cost numeric, od_id integer, node_orig integer, node_dest integer);"""

with engine.connect() as con:
    con.execute(command)

#ici il faut specifier le nom du schema et de la table osm
schema = 'public'
table_name = 'hh_2po_4pgr'

#ici il faut specifier le nom de la colonne de geometrie
geometry_column = 'geom_way'

for ix, row in paires_od.iterrows():
    print(ix) 

    # print row['node_orig']

    with engine.connect() as con:
        con.execute("DROP TABLE IF EXISTS od_chemins_tmp;")

    command = """CREATE TABLE od_chemins_tmp AS   
    (SELECT ST_Collect(a.geom) AS geom_chemin, string_agg(a.osm_id::varchar,',') AS seq_liens_osm, string_agg(a.edge::varchar,',') AS seq_liens, sum(cost) as cost  FROM
    (SELECT seq, node, edge, l.cost as cost, (ST_Dump({0})).geom AS geom, osm_id, id FROM pgr_dijkstra('
    SELECT id AS id,
      source::int4 AS source,
      target::int4 AS target,
      cost
    FROM {1}.{2}', {3}, {4}, true) AS l
    JOIN {1}.{2} AS r
    ON r.id = l.edge) a);""".format(geometry_column, schema, table_name, row['node_orig'], row['node_dest'] )
  
    with engine.connect() as con:
      con.execute(command)

    command = """ALTER TABLE od_chemins_tmp ADD COLUMN od_id integer;
    UPDATE od_chemins_tmp SET od_id = {0};
    ALTER TABLE od_chemins_tmp ADD COLUMN node_orig integer;
    UPDATE od_chemins_tmp SET node_orig = {1} ;
    ALTER TABLE od_chemins_tmp ADD COLUMN node_dest integer;
    UPDATE od_chemins_tmp SET node_dest = {2}; 
    INSERT INTO od_chemins (SELECT * FROM od_chemins_tmp);""".format(row['id'], row['node_orig'], row['node_dest'])

    with engine.connect() as con:
      con.execute(command)
  
now_2 = datetime.datetime.now()
delta_t = now_2 - now_1
print(delta_t)


#############################################################################################################################################################################################################################################################################################################################################