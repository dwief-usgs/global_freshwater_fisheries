
from utils import build_lk_catchment as build_lk
import geopandas as gpd
from multiprocessing import Pool
from functools import partial
import os

def run_lake(lake):
    if hasattr(lake, 'dir_file'):
        flow_dir_file = lake.dir_file
        print (f"processing: {lake}" )
        build_lk.watershed(flow_dir_file,lake,55)

if __name__ == '__main__':


    lake_file = 'data/HydroLAKES_polys_v10.gdb'
    lakes_df = gpd.read_file(lake_file, layer= 'HydroLAKES_polys_v10')

    #Create list of objects for each lake
    lakes_info = []
    for lake in lakes_df.itertuples():
        lake_id = lake.Hylak_id
        continent = lake.Continent
        wshd_area = lake.Wshd_area
        pour_pt_lat = lake.Pour_lat
        pour_pt_lon = lake.Pour_long
        geom = lake.geometry
        lk = build_lk.Lake(lake_id, continent, wshd_area, pour_pt_lat, pour_pt_lon, geom)
        lk.which_dir_raster()
        lakes_info.append(lk)
    


    dirName = "output/lkshed"
    # Create target directory & all intermediate directories if don't exists
    try:
        os.makedirs(dirName)    
        print("Directory " , dirName ,  " Created ")
    except FileExistsError:
        print("Directory " , dirName ,  " already exists") 

    for lake in lakes_info:
        if hasattr(lake, 'dir_file'):
            flow_dir_file = lake.dir_file
            print (f"processing: {lake}" )
            build_lk.watershed(flow_dir_file,lake,55)

    p = Pool(6)
    list_of_results = p.map(run_lake, ((lake for lake in lakes_info)))
    p.close()
    p.join()


        