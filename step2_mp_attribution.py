
from utils import file_management as f_mng
from utils import attribution as attr
import geopandas as gpd
from timeit import default_timer as timer
from functools import partial
from multiprocessing import Pool
import json

import warnings
warnings.filterwarnings("ignore")



def run_the_stats(gdf, json_file_info, basin_id):
    #Select row from geodataframe with basin information, including bounding box info
    basin_info_gdf = gdf.loc[gdf['HYBAS_ID']==basin_id]
    pfaf_12 = basin_info_gdf.iloc[0]['PFAF_ID']
    sub_area = float((basin_info_gdf.iloc[0])['SUB_AREA'])
    
    bbox_gdf = basin_info_gdf.bounds

    #Initiate basin stat object, this will help manage stats as created for the basin
    basin_info = attr.Stats(basin_id, pfaf_12, sub_area)

    #Add bounding box of basin to object
    basin_info.bounds((bbox_gdf['minx']),(bbox_gdf['maxx']),(bbox_gdf['miny']), (bbox_gdf['maxy']))
    # loop through each record (representing a file in the folder containing variable data) in json_file_info 
    for file in json_file_info:
        #bounds of tif file
        var_bounds = file['bounds']

        #evaluate if tif file bounds spatialy overlap basin, process if overlap/intersection occurs
        basin_info.evaluate_intersection(var_bounds)
        basin_info.run_zonal_stats(basin_info_gdf, file)
    
    final_basin_stats = basin_info.basin_stats
    final_basin_stats['id'] = int(basin_info.id)
    final_basin_stats['pfaf_12'] = int(basin_info.pfaf_12)
    final_basin_stats['sub_area'] = basin_info.sub_area

    return final_basin_stats


if __name__ == '__main__':

    start_all= timer()

    all_results = []
    regions = ['af','ar','as','au','eu','gr','na','sa','si']
    for region in regions:
        start_load= timer()

        #Import file processing information
        with open("data/var/tif_vars_file_info.json", "r") as all_file_info:
            json_file_info = json.load(all_file_info)

        #Read in pickled level 12 HydroBASINS (see step1_data_management.ipynb)
        gdf = f_mng.read_pkl_gdf(f'data/basins_{region}_lvl12_gdf.pkl')

        #Read in list of HYBAS_IDs (see step1_data_management.ipynb).  Provides list of IDs to process.
        basin_list = gdf['HYBAS_ID'].tolist()

        #If you want to run a small subset
        #basin_list = basin_list[0:450]

        end_load = timer() 
        print(f'Seconds taken to load data for region {region}: {end_load-start_load}') 


        start_proc= timer()
        
        # use 7 processers, leave () empty to use all available, noticed that sometimes causes issues on local machine
        p = Pool(7)
        values = partial(run_the_stats, gdf, json_file_info)
        result = p.map(values, basin_list)
        p.close()
        p.join()

        all_results += result

        end_proc = timer() 
        print(f'Seconds taken to process region {region}: {end_proc-start_proc}') 
    

    all_results_clean = [x for x in all_results if x]
    outfile_name = f'output/tif_hb12_att.json'
   
    with open(outfile_name, 'w') as outfile:
        json.dump(all_results_clean, outfile)

    end_all = timer() 
    print(f'Seconds taken to process all: {end_all-start_all}') 

    


        