#Import packages
import pandas as pd
import rasterio
from rasterstats import zonal_stats
import json
import csv


############################################################################################
############################################################################################
'''
    Author
    ---------
    Daniel Wieferich: dwieferich@usgs.gov

    Description
    ---------
    attribution of files

    Parameters
    ---------
    
    
'''
class Stats:
    def __init__(self, spatial_unit_id):
        self.id = spatial_unit_id
        self.basin_stats = {}
    
    def bounds(self, xmin, xmax, ymin, ymax):
        self.bounds = {'xmin':xmin, 'xmax':xmax, 'ymin':ymin, 'ymax':ymax}

    def evaluate_intersection(self, bounds_to_eval):
        if float(self.bounds['xmin'])>= float(bounds_to_eval['xmin']) and \
        float(self.bounds['ymin'])>= float(bounds_to_eval['ymin']) and \
        float(self.bounds['xmax'])<= float(bounds_to_eval['xmax']) and \
        float(self.bounds['ymax'])<= float(bounds_to_eval['ymax']):
            self.bounds_eval = 'bounds_contain_obj'
        elif (((float(self.bounds['xmin'])< float(bounds_to_eval['xmax']) and
        float(self.bounds['xmin'])> float(bounds_to_eval['xmin'])) or \
        (float(self.bounds['xmax'])> float(bounds_to_eval['xmin']) and
        float(self.bounds['xmax'])< float(bounds_to_eval['xmax']))) and \
        ((float(self.bounds['ymin'])< float(bounds_to_eval['ymax']) and
        float(self.bounds['ymin'])> float(bounds_to_eval['ymin'])) or \
        float(self.bounds['ymax'])> float(bounds_to_eval['ymin']) and
        float(self.bounds['ymax'])< float(bounds_to_eval['ymax']))):
            self.bounds_eval =  'bounds_intersect_obj'
        else:
            self.bounds_eval =  'no_intersection'

    #def run_stats(self, object_gdf, variable_file_path, stats, nodata_val, label):
    def run_stats(self, object_gdf, variable_file_path, stats, nodata_val, label, file_name):
        prefix = f'{label}_'
        #src_info = {'file_name': variable_file_path, 'bounds_eval': self.bounds_eval}
        src_info = {'file_name': file_name, 'bounds_eval': self.bounds_eval}
        result = zonal_stats(object_gdf, variable_file_path, stats=stats, nodata=nodata_val, geojson_out=False, prefix=prefix, band=1)
        result[0]['src_file']= [src_info]
        file_stats = {}
        file_stats[label]= result[0]
        #self.file_stats = file_stats

        if label in self.basin_stats:
            self.basin_stats[label]['src_file'].append(src_info)
            for stat in stats.split():
                stat_key_name = f'{label}_{stat}'
                if stat in ['count','nodata','sum']:
                    self.basin_stats[label][stat_key_name] += result[0][stat_key_name]
                elif stat == 'mean':
                    count_key = f'{label}_count'
                    current_count = self.basin_stats[label][count_key]
                    additional_count = result[0][count_key]
                    current_mean = self.basin_stats[label][stat_key_name]
                    additional_mean = result[0][stat_key_name]
                    area_weighted = ((current_count*current_mean)+(additional_count*additional_mean))/(current_count+additional_count)
                    self.basin_stats[label][stat_key_name] = area_weighted
                
        else:
            self.basin_stats.update(file_stats)

    #def to_csv(self, out_file_name):
    #    with open(out_file_name, 'a') as outfile:
    #        field_names = ['hybas_id']

    #def to_json_file(self, outfile_name):
    #    with open(outfile_name, 'w') as outfile:
    #        final_basin_stats: self.basin_stats
    #        final_basin_stats['id'] = self.id
    #        final_basin_stats.update

    def add_to_all_stats(self, collection):
            #final_basin_stats = {}
            final_basin_stats = self.basin_stats
            final_basin_stats['id'] = self.id
            collection.append(final_basin_stats)
            return collection




############################################################################################
############################################################################################