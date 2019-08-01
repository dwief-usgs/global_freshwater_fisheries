#Import packages
import pandas as pd
import rasterio
from rasterstats import zonal_stats
import json
#import csv


############################################################################################
############################################################################################
'''
    Author
    ---------
    Daniel Wieferich: dwieferich@usgs.gov

    Description
    ---------
    module that builds objects for attributing information from landscape variables to spatial units such as watersheds
'''
class Stats:
    def __init__(self, spatial_unit_id):
        '''
        Description
        ---------
        Initiates a statistic specific to a spatial unit, such as a watershed

        Parameters
        ---------
        spatial_unit_id : identifier of the spatial unit
        '''
        self.id = spatial_unit_id
        self.basin_stats = {}
    
    def bounds(self, xmin, xmax, ymin, ymax):
        '''
        Description
        ---------
        Captures spatial bounds of the object
        '''
        self.xmin = float(xmin)
        self.xmax = float(xmax)
        self.ymin = float(ymin)
        self.ymax = float(ymax)

    def evaluate_intersection(self, bounds_to_eval):
        '''
        Description
        ---------
        Evaluates if the objects bounds are contained within bounds of another spatial unit. Note the crs must be the same between both spatial features
        This is being used to understand the spatial relationship between the object and landscape data.  e.g. In some cases an object may span multiple grids
        of the same data. Also if no intersection is detected the processing intensive steps of attribution can be bipassed.

        Parameters
        ---------
        bounds_to_eval : dictionary of bounds that must contain xmin, xmax, ymin, ymax.  

        Output
        ---------
        self.bounds_eval : object attribute is assigned to 1 if object is completely within the bounds, 
        2 if the object's bounds are partially within the bounds and 0 if object does not intersect the bounds
        '''
        eval_xmin = float(bounds_to_eval['xmin'])
        eval_xmax = float(bounds_to_eval['xmax'])
        eval_ymin = float(bounds_to_eval['ymin'])
        eval_ymax = float(bounds_to_eval['ymax'])

        #Test if the object is contained within the bounds
        if (self.xmin >= eval_xmin and self.ymin >=eval_ymin and self.xmax <= eval_xmax and self.ymax <= eval_ymax):
            self.bounds_eval=1
        
        #Test if the object intersects the bounds but is not contained by bounds
        elif (((self.xmin<eval_xmax and self.xmin>eval_xmin) or (self.xmax>eval_xmin and self.xmax<eval_xmax)) and \
            ((self.ymin<eval_ymax and self.ymin>eval_ymin) or (self.ymax>eval_ymin and self.ymax<eval_ymax))):  #ymin falls within or ymax falls within
            self.bounds_eval =  2
        
        else:
            self.bounds_eval =  0

    
    def run_zonal_stats(self, object_gdf, file):
        '''
        Description
        ---------
        If object is contained within or intersects with bounds of extent of variable information then process zonal stats
        During zonal stats build out object
        If 2 or more scr files have data on same variable for the object then combine results 

        Parameters
        ---------
        object_gdf : geodataframe with spatial unit to process
        file: dictionary containing the following key value pairs of the data being attributed to the spatial object
            { 'label': 'short str describing variable being attributed',
             'no_data_val': value representing nodata in gridded src file,
             'var_file_path': ' str path to file including file name and type, e.g. /data/land_use.tif',
             'stats' : 'str of statistic to be calculated by zonal stats, if more than one have one space between each stat this code works with the following min max mean nodata count sum',
             'file_name': 'name and type but no directory, e.g. land_use.tif'}

             Note: nodata stat counts pixels with nodata

        Output
        ---------
        self.basin_stats : dictionary of zonal statistics
        '''

        if self.bounds_eval >0:
            label = file['label']
            nodata_val = file["no_data_val"]
            var_file_path = file["file_path"]
            stats = "nodata count mean"
            file_name = file["file_name"]
            prefix = f'{label}_'

            src_info = {'file_name': file_name, 'bounds_eval': self.bounds_eval}
            result = zonal_stats(object_gdf, var_file_path, stats=stats, nodata=nodata_val, geojson_out=False, prefix=prefix, band=1)
            result[0]['src_file']= [src_info]
            file_stats = {}
            file_stats[label]= result[0]

            #If a summary already exists for that label (from a different source file of same variable), combine stats
            if label in self.basin_stats:
                self.basin_stats[label]['src_file'].append(src_info)
                for stat in stats.split():
                    stat_key_name = f'{label}_{stat}'
                    new_val = result[0][stat_key_name]
                    current_val = self.basin_stats[label][stat_key_name]
                    if stat in ['count','nodata','sum']:
                        current_val += new_val
                    elif stat == 'mean':
                        count_key = f'{label}_count'
                        current_count = self.basin_stats[label][count_key]
                        additional_count = result[0][count_key]
                        if current_val is None:
                            current_val = float(0)
                        additional_mean = new_val
                        if additional_mean is None:
                            additional_mean = float(0)
                        if current_count+additional_count > 0:
                            area_weighted = ((float(current_count)*float(current_val))+(float(additional_count)*float(additional_mean))/(float(current_count)+float(additional_count)))
                        else:
                            area_weighted = float(0)
                        current_val = area_weighted
                    elif stat == 'min':
                        if current_val > new_val:
                            current_val = new_val
                    elif stat == 'max':
                        if current_val < new_val:
                            current_val = new_val

                    
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
            final_basin_stats = self.basin_stats
            final_basin_stats['id'] = self.id
            collection.append(final_basin_stats)
            return collection




############################################################################################
############################################################################################