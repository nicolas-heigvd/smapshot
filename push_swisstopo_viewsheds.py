#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 25 14:43:49 2018

@author: nicolas
"""

###############################################################################
import os, sys, glob
import numpy as np
import pandas as pd
import shapely
from shapely.geometry import mapping, shape, point, linestring
from shapely import  wkb, wkt
import geopandas as gpd
import psycopg2
import pyproj
import functools
from osgeo import ogr

###############################################################################
basedir   = '/path/to/local/Data/smapshot/swisstopo2/'
datadir   = os.path.join(basedir,'Data')
vwsddir   = os.path.join(basedir,'viewsheds')

datafile   = os.path.join(datadir,'terra.csv')
vwsdfile   = os.path.join(vwsddir,'viewshed.csv')

###############################################################################
# FUNCTIONS:
###############################################################################
def transform_geom_with_pyproj(geom, scrs, tcrs):
    import pyproj
    import functools
    #import shapely
    import shapely.ops as sp_ops
    project = functools.partial(pyproj.transform, pyproj.Proj(init='EPSG:'+
                                str(scrs)),pyproj.Proj(init='EPSG:'+str(tcrs)))
    geom_transformed = sp_ops.transform(project, geom)
    return geom_transformed

###############################################################################
# TRIGGERS
###############################################################################
env = 'PROD'

###############################################################################
# MAIN
###############################################################################
if env == 'DEV':
    db  = {'pwd': 'password',
           'dbn': 'dbname',
           'hst': 'server',
           'usr': 'username'}
elif env == 'PROD':
    db = {'pwd': 'password',
          'dbn': 'dbname',
          'hst': 'server',
          'usr': 'username'}
    
conn_string = "host='"+db['hst']+"' dbname='"+db['dbn']+"' user='"+db['usr']+"' password='"+db['pwd']+"'"
###############################################################################

df_terra    = pd.read_csv(datafile, delimiter=';') # <- terra.csv file
df_viewshed = pd.read_csv(vwsdfile, delimiter=';') # <- viewsheds.csv file

df_terra['IMAGE_UUID'] = df_terra['GLOBALID'].replace(['{','}'],'',regex=True)

df_tot = pd.merge(df_viewshed, df_terra, how='inner', on=['IMAGE_UUID'])
df_sub = df_tot[['IMAGE_UUID','INVENTORY_NUMBER','WKT']]
lentot = len(df_sub)

# DB connection:
###############################################################################
conn = psycopg2.connect(conn_string)
curs = conn.cursor()
updt_qry = """UPDATE public.images SET viewshed=ST_CollectionExtract(ST_MakeValid(ST_Transform(ST_SetSRID(%s::geometry,21781),4326)),3) WHERE original_id=(%s)"""

for i, row in df_sub.iterrows():
    print("Processing viewshed number: {}/{}".format(i,lentot))
    try:
        vwshd = ogr.CreateGeometryFromWkt(row[2])
        vwshd_wkb = vwshd.ExportToWkb().encode('hex')
#            vwshd = wkt.loads(row[2])
    except IOError:
        print("ERROR while processing viewshed number: {}/{}".format(i,lentot))
        raise
    
#    vwshd = transform_geom_with_pyproj(vwshd, 21781, 4326).to_wkt()#.wkb_hex
    curs.execute( updt_qry, (vwshd_wkb, str(row[1])) )
    conn.commit()

conn.close()



###############################################################################
# EOF
