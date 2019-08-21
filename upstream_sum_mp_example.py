from utils import network_calc as calc
from multiprocessing import Pool
import geopandas as gpd
import pandas as pd
import numpy as np
from functools import partial
from timeit import default_timer as timer
import h5py
import sys

class StreamSegment(object):
    """ Define a river segment """
    def __init__(self):
      """ Initialize the river segment """
      self.id=None
      self.area=None
      self.tot_area=None
      self.pfaf_12=None
      self.order=None
      self.up_seg_ids=None
    
  
def get(hdf_name,seg_id):
    """ Given a hdf5 file name, obtain the record for the specified idNumber """
    item=StreamSegment() # Init a river segment
    with h5py.File(hdf_name,'r') as h5: # Open the hdf5 database and grab the entry
      tmp=h5.get(str(seg_id))
      item.id=seg_id
      try:
        item.area=np.array(tmp.get('area'),dtype=np.float32)
        item.tot_area=np.array(tmp.get('tot_area'),dtype=np.float32)
        item.pfaf_12=np.array(tmp.get('pfaf_12'),dtype=np.int)
        item.order=np.array(tmp.get('order'),dtype=np.int8)
        item.up_seg_ids=np.sort(np.array(tmp.get('up_seg_ids'),dtype=np.int))
      except:
        print('The hdf database does not contain data for id number',seg_id)
        sys.exit()
    return item

def upstream_sum(np_basin_data, basin_id):
    hdf_name = 'data/hb12_netowrk.h5'
    seg_data = get(hdf_name, str(basin_id))
    np_subset = np_basin_data[np.where(np.isin(np_basin_data[:,0], seg_data.up_seg_ids))]
    stat = ((np_subset[:,1].sum()) + seg_data.area)
    basin_stat = {'seg_id':int(basin_id), 'tot_area_sqkm': stat}
    return basin_stat


if __name__ == '__main__':

    regions = ['af','ar','as','au','eu','gr','na','sa','si']
    reg_df_list = []
    list_of_hybas_ids = []
    for region in regions:
        file_name = f'data/HydroSHEDS/HydroBASINS/basins/hybas_{region}_lev12_v1c/hybas_{region}_lev12_v1c.shp'
        gdf = gpd.read_file(file_name)
        gdf = gdf[['HYBAS_ID','SUB_AREA','geometry']]
        regional_df = pd.DataFrame(gdf.drop(columns=['geometry']))
        regional_id_list = (regional_df['HYBAS_ID']).to_list()
        list_of_hybas_ids += regional_id_list
        reg_df_list.append(regional_df)
    basin_data = pd.concat(reg_df_list)
    print ('done with create list')
    
    #convert df to numpy
    np_basin_data = basin_data.to_numpy()
    np_basin_data = np_basin_data[np_basin_data[:,0].argsort()]

    list_of_hybas_ids_sub = list_of_hybas_ids[0:50]
    
    start= timer()
    
    # use 7 processers, leave () empty to use all available
    p = Pool(7)
    list_agg_values = partial(upstream_sum, np_basin_data)
    result = p.map(list_agg_values, list_of_hybas_ids_sub)
    p.close()
    p.join()
    

    end = timer() 
    print("Time taken:", end-start) 

    out_data_df = pd.DataFrame(result)
    out_data_df.to_csv('output/test_up_area.csv', sep=',')


        