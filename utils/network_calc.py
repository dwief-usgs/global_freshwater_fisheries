#Import packages
from utils import file_management as f_mng
import pandas as pd
import numpy as np
import h5py
import sys


############################################################################################
############################################################################################
'''
    Author
    ---------
    Daniel Wieferich: dwieferich@usgs.gov

    Description
    ---------
    methods to help support upstream summaries of variables, these methods currently include target basin  
'''
class BasinInfo(object):
    """ Define a river segment """
    def __init__(self):
        """ Initialize the river segment """
        self.id=None
        self.area=None
        self.tot_area=None
        self.pfaf_id=None
        self.order=None
        self.up_seg_ids=None

def get(hdf_name, basin_id):
    """ Given a hdf5 file name, obtain the record for the specified idNumber """
    item=BasinInfo() # Init a river segment
    with h5py.File(hdf_name,'r') as h5: # Open the hdf5 database and grab the entry
      tmp=h5.get(str(basin_id))
      item.id=basin_id
      try:
        item.area=np.array(tmp.get('area'),dtype=np.float32)
        item.tot_area=np.array(tmp.get('tot_area'),dtype=np.float32)
        item.pfaf_id=np.array(tmp.get('pfaf_id'),dtype=np.int)
        item.order=np.array(tmp.get('order'),dtype=np.int8)
        item.up_seg_ids=np.sort(np.array(tmp.get('up_seg_ids'),dtype=np.int))
      except:
        print(f'The hdf database does not contain data for id number {basin_id}.')
        sys.exit()
    return item


def add_field_from_hb12(df, field_name, df_hybas_id='HYBAS_ID'):
    '''
    Description: adds field(s) from hb12 that was defined by user.  Intended to add area or length for weighting of summaries
    Parameters:
    df: pandas dataframe containing hybas_ids and variables
    field_name: str, name of column that needs to be merged into df.  multiple fields can be defined but must be comma delimited
    df_hybas_id: str, this is the join field of ids, included as variable in case name changed 
    '''
    hb12_df = f_mng.read_pkl_df(file_path='data/basins_lvl12_df.pkl')
    hb12_df = hb12_df[['HYBAS_ID', field_name]]
    df_merge = df.merge(hb12_df, how='left', left_on=df_hybas_id, right_on='HYBAS_ID')
    return df_merge

def get_local_var_weight(local_var_df, var_cols, id_col='HYBAS_ID', weight_col=None):
    '''
    Desription: Intermediate step that weights each value and preps for area or length weighted summaries
    Parameters: 
    var_cols: list of str, column names that need to be weighted
    weight: str, column name of weight.  most common will be area or length
    '''
    
    #if no weight col, prep data without weighting (ex for max, min, sum stats)
    if weight_col is not None:
        local_var_df[weight_col].astype(float)
        for col in var_cols:
            local_var_df[col].astype(float)
            local_var_df[col] = local_var_df[col]*local_var_df[weight_col]

    #insert id at index 0
    var_cols.insert(0,id_col)

    #select only id col and cols to summarize 
    local_var_df = local_var_df[var_cols]

    #convert df to numpy
    np_basin_local_var_df = local_var_df.to_numpy()
    #index by id 
    np_basin_local_var_df = np_basin_local_var_df[np_basin_local_var_df[:,0].argsort()]

    #should write test to make sure col_names = var_cols as submitted
    col_names = local_var_df.columns.tolist()
    #remove id from column list, leaving only variables to calculate
    col_names = col_names[1:]

    return np_basin_local_var_df, col_names


def upstream_summary(np_basin_local_var_data, col_names, summary_type, basin_id):
    hdf_name='output/hb12_network.h5'
    #get basin info from hdf
    basin_info = get(hdf_name, str(int(basin_id)))
    #get list of upstream basin ids, add target id to list
    upstream_basins = basin_info.up_seg_ids.tolist()
    #Add target basin to list
    upstream_basins.append(int(basin_info.id))

    #select local var data for all basins in list
    np_subset = np_basin_local_var_data[np.where(np.isin(np_basin_local_var_data[:,0], upstream_basins))]
    stats = np.sum(np_subset[:, 1:], axis=0)

    basin_upstream_stats = {}
    basin_upstream_stats['hybas_id'] = int(basin_info.id)
    basin_upstream_stats['pfaf_id'] = int(basin_info.pfaf_id)
    num_columns = len(col_names)
    i = 0
    while i < num_columns:
        if summary_type == 'area_weighted_mean':
            upstream_val = float(stats[i])/ float(basin_info.tot_area)
        elif summary_type == 'sum':
            upstream_val = float(stats[i])
        in_field_name = (col_names)[i]
        out_field_name = f'{in_field_name}_up'
        basin_upstream_stats[out_field_name] = upstream_val
        i+=1
      
    return basin_upstream_stats


############################################################################################
############################################################################################
