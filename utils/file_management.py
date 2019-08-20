'''
    Author
    ---------
    Daniel Wieferich: dwieferich@usgs.gov

    Description
    ---------
    Contains methods to gather, build and organize information about files to use for processing steps
    Currently included:
    1)Gather information about files in directory
    2)Build HydroSHEDS data
'''


#Import packages
import os
import pandas as pd
import rasterio
import geopandas as gpd
import pickle
import sys
import json

###############################################################################################
#The below section includes methods to gather and store information about files in a directory
###############################################################################################

'''
Description
---------
builds object with information about files
class is intended to be used with find_files function 

Parameters
---------
file_name : str representing the file name, no path but includes file type extension (e.g. 'file_name.shp')
file_path : str representing path to file within global_freshwater_fisheries repo structure, includes file_name (e.g. 'data/var/file_name.shp')
'''

class FileInfo:
    def __init__(self, file_name, file_path):
        self.file_name = file_name
        self.file_path = file_path

    def tif_info(self):
        if (self.file_name).endswith('.tif'): 
            try:
                file_data = rasterio.open(self.file_path)
                xmin, ymin, xmax, ymax = file_data.bounds
                self.bounds = {'xmin': xmin, 'xmax':xmax, 'ymin':ymin, 'ymax':ymax}
                self.no_data_val = file_data.nodata
                #rasterio geotransform, produces values ()
                gt = file_data.transform
                self.pixel_size = gt[0]
                self.crs = file_data.crs
            except:
                pass

    def user_supplied_info(self, csv_file='data/var/file_processing_info.csv'):
        '''
        Description
        ---------
        adds user supplied information to FileInfo objects

        Parameters
        ---------
        missing_info: list of arrays documenting which files are available but have missing information 
        '''
        missing_info = {}
        try:
            df = pd.read_csv(csv_file)
            df_row = df.loc[df['file_name']==self.file_name]
            rows, cols = df_row.shape
            if rows > 1:
                print (f'{self.file_name} is duplicated in data/var/src_processing_info.csv')
            elif rows < 1:
                missing_info['file_name']=self.file_name
                missing_info['missing_fields']=['field_name','variable', 'src_short','summary_type','label','all_touched','conditional']
                return missing_info
            else:
                #if any nan in df
                if len(df_row.columns[df_row.isna().any()].tolist())>0:
                    #add file name to missing data 
                    missing_info['file_name']=self.file_name
                    #add list of columns with missing data in src_processing_info.csv for this file
                    missing_info['missing_fields']=df_row.columns[df_row.isna().any()].tolist()
                    return missing_info
                else:
                    self.variable = df_row.iloc[0]['variable']
                    self.src_short = df_row.iloc[0]['src_short']
                    self.summary_type = df_row.iloc[0]['summary_type']
                    self.label = df_row.iloc[0]['label']
                    self.categorical = df_row.iloc[0]['categorical']
                    self.pixel_inclusion = df_row.iloc[0]['pixel_inclusion']
            
        except:
            missing_info['file_name']=self.file_name
            missing_info['file_failed']= True
            return missing_info

def store_file_info(file_list, directory, short_name):
    all_file_info = []
    missing_info = []
    for file in file_list:
        file_name = file['file_name']
        file_path = file['file_path']
        file_info = FileInfo(file_name, file_path)
        missing_data = file_info.user_supplied_info()
        if missing_data:
            missing_info.append(missing_data) 
        #Append only if file info is complete
        if hasattr(file_info, 'variable'):
            all_file_info.append(file_info.__dict__)

    outfile_name = f'{directory}/{short_name}_file_info.json'

    with open(outfile_name, 'w') as outfile:        
        json.dump(all_file_info, outfile)
    
    return all_file_info, missing_info



def find_files(directory='data/var', prefix=None, suffix=None):
    '''
    Description
    ---------
    create list of files in specified directory and having specified prefix and suffix

    Parameters
    ---------
    directory: str, directory to search
    prefix: str that file starts with
    suffix: str that file name ends with. must include file extension, for example search for file ending in '_v1c.shp' or a tiff file type using '.tif'.  
    '''
    file_list = []
    for root, dirs, files in os.walk(directory):
        if prefix is None and suffix is None:
            file_list += [{"file_path":(os.path.join(root, file)),"file_name":file} for file in files]
        elif prefix is None and suffix is not None:
            file_list += [{"file_path":(os.path.join(root, file)),"file_name":file} for file in files if file.endswith(suffix)]
        elif suffix is None and prefix is not None:
            file_list += [{"file_path":(os.path.join(root, file)),"file_name":file} for file in files if file.startswith(prefix)]
        elif suffix is not None and prefix is not None:
            file_list += [{"file_path":(os.path.join(root, file)),"file_name":file} for file in files if file.startswith(prefix) and file.endswith(suffix)]
    return file_list, directory


###############################################################################################
#The below section includes methods to build and store HydroSHEDS data for efficient use in processing steps
###############################################################################################

def build_basin_data(level='12', version='v1c', directory = 'data/HydroSHEDS'):
    '''
    Description
    ---------
    create pickle files to quickly read in HydroBASINS standard basin data in geodataframe, dataframe
    currently assumes set regions (see region variable) and file types (.shp) based on v1c standard basins

    Parameters
    ---------
    level: str, HydroBASINS PFAF level acceptable values ['02','03','04','05','06','07','08','09','10','11','12'], default = '12'
    version: version of HydroBASINS as denoted in file_name  
    
    Output
    ---------
    f'data/basins_lvl{level}_gdf.pkl' : pickle file of basin attributes for specified basin level with geometry for creating global gdf, read using def read_pkl_gdf
    f'data/basins_lvl{level}_df.pkl' : pickle file of basin attributes for specified basin level without geometry for creating global df, read using def read_pkl_df
    f'data/basins_lvl{level}.txt' : pickle file of full list of HydroBASIN ids for specified basin level, read using def read_pkl_df
    print : currently reports to user via print if wrong number of regional files detected, and if wrong number of identifiers is detected
    '''
    
    regions = ['af','ar','as','au','eu','gr','na','sa','si']
    
    #ensure level is string
    level = str(level)
    
    #search HydroSHEDS data
    prefix = 'hybas_'
    suffix = f'_lev{level}_{version}.shp'
    files, directory = find_files(directory, prefix, suffix)

    
    #test to see if to many files are returned, kill process
    if len(files) > len(regions):
        print (f'There should be {str(len(regions))} files, there are {str(len(files))}')
        if len(files) < 50:
               print (f'Available files include: {files}')
        sys.exit()

    else:
        #test if not all regions using count... could be improved to return acutal missing regions        
        if len(files) < len(regions):
            missing = str(len(regions)-len(files))
            print (f'Missing {missing} regional files.')
            print (f'Still running on available files include: {files}')

        list_hybas_ids = []
        reg_df_list = []
        for file in files:
            file_path = file['file_path']
            try:
                regional_gdf = gpd.read_file(file_path)
            except:
                print (f'Failed to build geodataframe for: {file_path}')
            
            try:
                #Create regional basin dataframe, add to global list of dataframes
                regional_df = pd.DataFrame(regional_gdf)
                reg_df_list.append(regional_df)
                #Create list of ids in regional list and add to global list
                regional_id_list = (regional_df['HYBAS_ID']).to_list()
                list_hybas_ids += regional_id_list
            except:
                print (f'Failed to convert to dataframe and list for: {file_path}')

        if len(list_hybas_ids) != 1034083:
            print (f'Warning, expected 1034083 ids, you have {str(len(list_hybas_ids))}')
                 
        try:
            #pickle list of hydrobasin ids
            list_hybas_ids = list(set(list_hybas_ids))
            print (len(list_hybas_ids))
            outfile_list_ids = f'data/basins_lvl{level}.txt'
            with open(outfile_list_ids, "wb") as f:   
                pickle.dump(list_hybas_ids, f)
                
            #pickle dataframe of basin data for all regions
            basin_data_w_geom = pd.concat(reg_df_list) 
            outfile_basin_gdf = f'data/basins_lvl{level}_gdf.pkl' 
            basin_data_w_geom.to_pickle(outfile_basin_gdf)
            basin_data = basin_data_w_geom.drop(columns=['geometry'])
            outfile_basin_df = f'data/basins_lvl{level}_df.pkl' 
            basin_data.to_pickle(outfile_basin_df)

        except:
            print (f'Failed to pickle files')

    
def read_pkl_gdf(file_path='data/basins_lvl12_gdf.pkl', crs={'init':'epsg:4326'}):
    temp_df = pd.read_pickle(file_path)
    gdf = gpd.GeoDataFrame(temp_df, crs=crs, geometry='geometry')
    return gdf

def read_pkl_df(file_path='data/basins_lvl12_df.pkl'):
    df = pd.read_pickle(file_path)
    return df



############################################################################################
############################################################################################