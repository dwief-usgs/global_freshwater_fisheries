
from utils import file_management as f_mng
from utils import attribution as attr
import geopandas as gpd
from timeit import default_timer as timer
from functools import partial
from multiprocessing import Pool
import json



def run_the_stats(gdf_w_bbox, json_file_info, basin_id):
    #Select row from geodataframe with basin information, including bounding box info
    basin_info_gdf = gdf_w_bbox.loc[gdf_w_bbox['HYBAS_ID']==basin_id]

    #Initiate basin stat object, this will help manage stats as created for the basin
    basin_info = attr.Stats(basin_id)

    #Add bounding box of basin to object
    basin_info.bounds((basin_info_gdf['minx']),(basin_info_gdf['maxx']),(basin_info_gdf['miny']), (basin_info_gdf['maxy']))
    # loop through each record (representing a file in the folder containing variable data) in json_file_info 
    f = 0
    for file in json_file_info:
        f +=1
        # If the variable file is flagged as 1 we want to process it
        if 'to_summarize' in file and file['to_summarize']==1:
            var_bounds = file['bounds']
            #evaluate if data in file spatialy overlaps spatial object using bounds, process if overlap/intersection occurs
            basin_info.evaluate_intersection(var_bounds)
            
            basin_info.run_zonal_stats(basin_info_gdf, file)
    
    final_basin_stats = basin_info.basin_stats
    final_basin_stats['id'] = basin_info.id

    return final_basin_stats


if __name__ == '__main__':

    

    with open("data/var/files_info.json", "r") as all_file_info:
        json_file_info = json.load(all_file_info)

    gdf = gpd.read_file('data/HydroSHEDS/HydroBASINS/basins/hybas_as_lev12_v1c/hybas_as_lev12_v1c.shp') 

    start= timer()

    #create list of basin ids that need processing
    basin_list = gdf['HYBAS_ID'].tolist()
    basin_list = basin_list[0:3000]

    #create bounding box dataframe and join back to gdf using index
    bbox= gdf.bounds
    gdf_bbox = gdf.join(bbox)
    gdf_bbox.head()
    
    # use 7 processers, leave () empty to use all available
    p = Pool(7)
    values = partial(run_the_stats, gdf_bbox, json_file_info)
    result = p.map(values, basin_list)
    p.close()
    p.join()
    
    #final_list = [item for sublist in result for item in sublist] 
    with open('test_stats.json', 'w') as outfile:
        json.dump(result, outfile)

    end = timer() 
    print("Time taken:", end-start) 


        