import pandas as pd
import numpy as np
from multiprocessing import Pool
from time import process_time
import h5py
import sys
from functools import partial
import datetime


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

def upstream_sum(basin_data_df):
    basin_data_agg = []

    stream_segment= get(hdf_name, row[0])    
    np_subset = np_non_headwaters[np.where(np.isin(np_non_headwaters[:,0], stream_segment.up_seg_ids))]
    upstream_area = (np_subset[:,1].sum()) + (row[1])
    non_headwater_agg.append({'seg_id':float(row[0]), 'tot_area_sqkm': upstream_area})
   
    return non_headwater_agg