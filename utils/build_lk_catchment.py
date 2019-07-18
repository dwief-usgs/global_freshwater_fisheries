#Import packages
import gc
import geopandas as gpd
import gdal
import numpy as np
import pysal as ps
import pandas as pd
import georasters as gr
import rasterio

############################################################################################
############################################################################################
'''
    Author
    ---------
    Daniel Wieferich: dwieferich@usgs.gov

    Description
    ---------
    creates an object with information about a lake 
    original use with HydroLakes dataset
    
    '''
class Lake:
    def __init__(self, lake_id, continent, wshd_area, pour_pt_lat, pour_pt_lon, geom):
        self.id = int(lake_id)
        self.continent = str(continent)
        self.wshd_area = float(wshd_area)
        self.pour_pt_lat = float(pour_pt_lat)
        self.pour_pt_lon = float(pour_pt_lon)
        self.geom = geom
        
    def __repr__(self):
        return f"Lake({self.id},{self.continent},{self.wshd_area},{self.pour_pt_lat},{self.pour_pt_lon})"
    
    def __str__(self):
        return f"lake_id: {self.id}, continent: {self.continent}, wshd_area: {self.wshd_area}, wgs84_xy:{self.pour_pt_lon}, {self.pour_pt_lat})"

    def which_dir_raster(self, raster_file_dir= ['data/HydroSHEDS/HydroBASINS/dir/na/na_dir_15s','data/HydroSHEDS/HydroBASINS/dir/af/af_dir_15s', \
    'data/HydroSHEDS/HydroBASINS/dir/au/au_dir_15s', 'data/HydroSHEDS/HydroBASINS/dir/ca/ca_dir_15s', 'data/HydroSHEDS/HydroBASINS/dir/eu/eu_dir_15s', \
    'data/HydroSHEDS/HydroBASINS/dir/sa/sa_dir_15s','data/HydroSHEDS/HydroBASINS/dir/as/as_dir_15s']):
        for raster in raster_file_dir:
            dataset = rasterio.open(raster)
            xmin,ymin,xmax,ymax = dataset.bounds
            if self.pour_pt_lat > ymin and self.pour_pt_lat < ymax and self.pour_pt_lon > xmin and self.pour_pt_lon<xmax:
                self.dir_file = raster
                break




############################################################################################
############################################################################################
"""
Created on Tue Feb 21 12:55:48 2017
@author: Rdebbout

Altered on July 16 2019
@author: dwief-usgs
effort: Updated methods to use py3.x packages
altered some steps like reduce size to work against unprojected data
updated documentation
"""

#Flow directions values to test when working with HydroSHED data (and many others): 
#Key represents cardinal direction of position from target position in numpy array
# 0:East, 1:SouthEast, 2:South, 3:SouthWest, 4:West, 5:NorthWest, 6:North, 7:Northeast 
#Value represents flow direction 
# 1: flows East, 2: flows SouthEast, 4: flows south, 8: flows SouthWest, 16: flows West
# 32: flows NorthWest, 64: flows North, 128: flows NorthEast
DIRS = {0:16,1:32,2:64,3:128,4:1,5:2,6:4,7:8}


def ring_cells(target_cell):
    '''
    Description
    ---------
    Identifies positions of grid cell directly surrounding the target grid cell. 
    
    Parameters
    ---------
    target_cell   : targeted index position to inspect within a numpy array.  format column, row

    Output
    ---------
    return        : list of index positions in numpy array that directly surround target index position.
                    note that the sequence of positions is important when using eval_ring
    '''
    zero = (target_cell[0],target_cell[1]+1)            # cell east of target 
    one = (target_cell[0]+1,target_cell[1]+1)           # cell southeast of target
    two = (target_cell[0]+1,target_cell[1])             # cell south of target
    three = (target_cell[0]+1,target_cell[1]-1)         # cell southwest of target
    four = (target_cell[0],target_cell[1]-1)            # cell west of target
    five = (target_cell[0]-1,target_cell[1]-1)          # cell northwest of target
    six = (target_cell[0]-1,target_cell[1])             # cell north of target
    seven = (target_cell[0]-1,target_cell[1]+1)         # cell northeast of target
    return [zero,one,two,three,four,five,six,seven] 

def eval_ring(ring,ras):
    '''
    __orig_author__ = rdebbout

    Description
    --------
    Evaluate values for index positions in a numpy array that relate to flow direction.  If flowing towards target cell capture index position in list.

    Output
    ---------
    hold    :   list of index positions that tested true
    
    '''
    temporary_hold = []
    for c in DIRS:
        if ras.raster[ring[c]] == DIRS[c]:
            temporary_hold.append(ring[c])

    return temporary_hold 

def make_rat(fn):
    '''
    __orig_author__ =  "Matt Gregory <matt.gregory@oregonstate.edu >"
    Adds a Raster Attribute Table to the .tif.aux.xml file, then passes those
    values to rat_to_df function to return the RAT in a pandas DataFrame.
    
    Arguments
    ---------
    fn       : raster filename

    Output
    ---------
    df       : pd.DataFrame with raster attribute table information
    '''
    ds = gdal.Open(fn)
    rb = ds.GetRasterBand(1)
    nd = rb.GetNoDataValue()
    data = rb.ReadAsArray()
    # Get unique values in the band and return counts for COUNT val
    u = np.array(np.unique(data, return_counts=True))
    #  remove NoData value
    u = np.delete(u, np.argwhere(u==nd), axis=1)
    
    # Create and populate the RAT
    rat = gdal.RasterAttributeTable()
    rat.CreateColumn('Value', gdal.GFT_Integer, gdal.GFU_Generic)
    rat.CreateColumn('Count', gdal.GFT_Integer, gdal.GFU_Generic)
    for i in range(u[0].size):
        rat.SetValueAsInt(i, 0, int(u[0][i]))
        rat.SetValueAsInt(i, 1, int(u[1][i]))
    
    # Associate with the band
    rb.SetDefaultRAT(rat)
    
    # Close the dataset and persist the RAT
    ds = None
    rb = None
    nd = None
    data = None

    #return the rat to build DataFrame
    df = rat_to_df(rat)

    return df
       
##############################################################################


def rat_to_df(in_rat):
    """
    __orig_author__ =  "Matt Gregory <matt.gregory@oregonstate.edu >" 
    Given a GDAL raster attribute table, convert to a pandas DataFrame

    Parameters
    ----------
    in_rat : gdal.RasterAttributeTable
        The input raster attribute table

    Returns
    -------
    df : pd.DataFrame
        The output data frame
    """
    # Read in each column from the RAT and convert it to a series inferring data type automatically
    
    #Updated xrange to range from original version for use in Python 3 
    s = [pd.Series(in_rat.ReadAsArray(i), name=in_rat.GetNameOfCol(i)) for i in range(in_rat.GetColumnCount())]

    # Concatenate all series together into a dataframe and return
    return pd.concat(s, axis=1)
       
##############################################################################

def df2dbf(df, dbf_path, my_specs=None):
    '''
    Convert a pandas.DataFrame into a dbf.
    __orig_author__ = "Dani Arribas-Bel <darribas@asu.edu> "
    ...
    Arguments
    ---------
    df          : DataFrame
                  Pandas dataframe object to be entirely written out to a dbf
    dbf_path    : str
                  Path to the output dbf. It is also returned by the function
    my_specs    : list
                  List with the field_specs to use for each column.
                  Defaults to None and applies the following scheme:
                    * int: ('N', 14, 0)
                    * float: ('N', 14, 14)
                    * str: ('C', 14, 0)
    '''
    if my_specs:
        specs = my_specs
    else:
        type2spec = {int: ('N', 20, 0),
                     np.int64: ('N', 20, 0),
                     float: ('N', 36, 15),
                     np.float64: ('N', 36, 15),
                     str: ('C', 14, 0),
                     np.int32: ('N', 14, 0)
                     }
        types = [type(df[i].iloc[0]) for i in df.columns]
        specs = [type2spec[t] for t in types]
    
    #dw: Python 3 looks like pysal changed package structure
    db=ps.lib.io.open(dbf_path, 'w')
    db.header = list(df.columns)
    db.field_spec = specs
    for i, row in df.T.iteritems():
        db.write(row)
    db.close()
    return dbf_path    

def watershed(flow_dir_file, lake_data, val=47):
    """
    __orig_author__ =  Rdebbout
    
    Description
    ----------
    given a flow direction grid, develop tif representing lake watershed

    Parameters
    ----------
    flow_dir_file : grid file representing flow direction.  DIR file within HydroSHEDS
        
    lake_data   : object representing lake information including id and pour point lat/lon

    val : value to set watershed cells 

    Output
    ---------
    tif file of a lake's watershed including value attribute table
    
    """
   
    #get information about original flow direction raster
    #NDV = no data values    , xysize= raster size , tf = geotransform
    NDV, xsize, ysize, tf, Projection, DataType = gr.get_geo_info(flow_dir_file)
    #print (tf)

    #load into georaster from source file
    arr = gr.from_file(flow_dir_file)
    (xmin, xsize, x, ymax, y, ysize) = arr.geot

    #get pour point lon and lat from lake object
    pp_lon,pp_lat = lake_data.pour_pt_lon, lake_data.pour_pt_lat
    
    # Function to map location in pixel of raster array: https://github.com/ozak/georasters/blob/master/georasters/georasters.py
    here = gr.map_pixel(pp_lon,pp_lat,xsize,ysize,xmin,ymax)
    init = []
    init = ring_cells(here)
    
    hold = []
    temp_hold = eval_ring(init,arr)
    hold = temp_hold

    #Altered this to deal with an issue when iterating across multiple lakes in original code
    #for every cell in ring evaluate if it is flowing to the pour point, if so add it to the hold, temp_hold continues until exhausted
    while len(temp_hold)>0:
        for h in temp_hold:
            init = ring_cells(h)
            temp_hold = eval_ring(init,arr)
            hold += temp_hold
            
    #Add target cell
    hold.append(here)
    hold = list(set(hold))
    
    # try to update transform info(tf) and shrink array
    dd = np.array(hold)
    r_min, col_min = np.amin(dd,axis=0)
    r_max, col_max = np.amax(dd,axis=0)
    
    #added step for WGS84 data that is not projected
    lon, lat = gr.map_pixel_inv(r_min, col_min, tf[1], tf[-1], tf[0], tf[3])    
    
    # shift transform to reduce NoData in the output raster
    shifted_tf = (lon ,tf[1],tf[2], lat, tf[4],tf[5])
  
    # insert val into the cells of the raster that represent watershed
    out = np.zeros(arr.shape)
    for idx in hold:
        out[idx] = val
    
    #plus 1 deals with index starting at 0,  these are counts of rows, columns
    new = out[r_min:(r_max+1),col_min:(col_max+1)]  
  
    go = gr.GeoRaster(new, shifted_tf, 0)
    go.projection = Projection
    go.datatype = DataType    
    
    #Set name of files and create
    lk_id = str(lake_data.id)

    #create tif file
    go.to_tiff(f"output/lkshed/{lk_id}_watershed")
    df = make_rat(f"output/lkshed/{lk_id}_watershed.tif")
    #export value attribute table
    df2dbf(df, f"output/lkshed/{lk_id}_watershed.tif.vat.dbf")

    # #cleanup to help take care of issues on iterative processing
    # arr = 0
    go = None
    here = None
    arr = None
    out = None
    new = None
    dd = None
    shifted_tf = None
    gc.collect()

