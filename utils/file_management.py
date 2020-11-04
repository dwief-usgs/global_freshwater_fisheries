"""Manage information about files for attribution process.

    Author
    ----------
    Daniel Wieferich: dwieferich@usgs.gov

    Description
    ----------
    Contains methods to gather, build and organize information about files
    to use for processing steps in global assessment work

    Currently included:
    1)Gather information about files in directory
    2)Build HydroSHEDS data
"""

# Import packages
import os
import pandas as pd
import rasterio
import geopandas as gpd
import pickle
import sys
import json
from pathlib import Path


class FileInfo:
    """Includes methods to get information about files.

        Description
        ----------
        Builds object to describe a file.
        This class is intended to be used with find_files function (below).

        Parameters
        ----------
        file_name : str
            string representation of file name
            does not include path but includes file type extension
            Examples include 'file_name.shp' or 'file_name.tif'
        file_path : str
            string representation of file path
            includes file_name (e.g. 'data/var/file_name.shp')

    """
    def __init__(self, file_name, file_path):
        """Initiate object to represent a file for processing."""
        self.file_name = file_name
        self.file_path = file_path
        self.file_exists = False  # false until verified
        self.meets_criteria = False
        self.in_control_file = False
        self.missing_info = False  # in control file

    def get_file_info_from_user(
            self,
            control_file='data/var/file_processing_info.csv'):
        """Adds user supplied information from control file to FileInfo object.

        Description
        ----------
        Requires a user populated control file with a row detailing
        information about each geospatial file to process.
        This function uses the control file to add
        attributes to the FileInfo object.

        Parameters
        ----------
        missing_info: list of arrays
            list of arrays documenting which files are available but
            are missing information needed for assessment processing

        """
        the_file = Path(control_file)
        if the_file.is_file():
            self.file_exists = True
            df = pd.read_csv(control_file)
            df_row = df.loc[df['file_name'] == self.file_name]

            rows = df_row.shape[0]
            if rows > 1:
                self.in_control_file = True
                self.message = f"{self.file_name} duplicated in control_file"
            elif rows < 1:
                self.in_control_file = True
                self.message = f"{self.file_name} is not in control_file"
            elif len(df_row.columns[df_row.isna().any()].tolist()) > 0:
                missing_cols = df_row.columns[
                    df_row.isna().any()
                ].tolist()
                m = f"{self.file_name} has missing fields {missing_cols}"
                self.message = m
                self.in_control_file = True
                self.missing_info = True
            else:
                self.variable = df_row.iloc[0]['variable']
                self.src_short = df_row.iloc[0]['src_short']
                self.summary_type = df_row.iloc[0]['summary_type']
                self.label = df_row.iloc[0]['label']
                self.categorical = df_row.iloc[0]['categorical']
                self.pixel_inclusion = df_row.iloc[0]['pixel_inclusion']
                self.meets_criteria = True

    def tif_info(self):
        """Get information about tif to direct processing."""
        if (self.file_name).endswith('.tif'):
            file_data = rasterio.open(self.file_path)
            xmin, ymin, xmax, ymax = file_data.bounds
            self.bounds = {'xmin': xmin,
                           'xmax': xmax,
                           'ymin': ymin,
                           'ymax': ymax
                           }
            self.no_data_val = file_data.nodata
            # rasterio geotransform, produces values
            gt = file_data.transform
            # document pixel size (spatial resolution) of data in file
            self.pixel_size = gt[0]
            # document coordinate reference system
            self.crs = str(file_data.crs)


def get_details_all_files(file_list, directory, short_name):
    """Build collection describing list of files to process.

    Description
    ----------
    Documents processing information for all files in a list.
    Returns JSON representation to pass as variable
    and exports to JSON file for documentation purposes

    Parameters
    ----------
    file_list: list
        list of strings
        for this effort list is generated using find_files function
    directory: str
        string representing directory where files are located
        for this effort list is generated using find_files function
    short_name: str
        short string to be used in naming output file

    """
    all_file_info = []
    for file in file_list:
        file_name = file['file_name']
        file_path = file['file_path']
        file_info = FileInfo(file_name, file_path)
        file_info.tif_info()
        file_info.get_file_info_from_user()
        all_file_info.append(file_info.__dict__)

    outfile_name = f'{directory}/{short_name}_file_info.json'

    with open(outfile_name, 'w') as outfile:
        json.dump(all_file_info, outfile)

    return all_file_info


def find_files(directory='data/var', prefix=None, suffix=None):
    """Create list of files in specified directory.

    Description
    ----------
    For given directory find all files with a given prefix and or suffix

    Parameters
    ----------
    directory: str
        directory to search
    prefix: str
        string that file name starts with
    suffix: str
        string that file name ends with and must include file extension
        for example search for file ending in '_v1c.shp'
        or a tiff file type using '.tif'

    Consideration
    ----------
        why not record directory in file_list to allow for
        searching of multiple directories

    """
    file_list = []
    for root, dirs, files in os.walk(directory):
        if prefix is None and suffix is None:
            file_list += [
                {"file_path": (os.path.join(root, file)),
                 "file_name": file,
                 } for file in files
            ]
        elif prefix is None and suffix is not None:
            file_list += [
                {"file_path": (os.path.join(root, file)),
                 "file_name": file
                 } for file in files if file.endswith(suffix)
            ]
        elif suffix is None and prefix is not None:
            file_list += [
                {"file_path": (os.path.join(root, file)),
                 "file_name": file
                 } for file in files if file.startswith(prefix)
            ]
        elif suffix is not None and prefix is not None:
            file_list += [
                {"file_path": (os.path.join(root, file)),
                 "file_name": file
                 } for file in files if file.startswith(prefix)
                and file.endswith(suffix)
            ]
    return file_list, directory


##################################################################
# The below section includes methods to build and store HydroSHEDS
# data for efficient use in processing steps
##################################################################

def build_basin_data(level='12',
                     version='v1c',
                     directory='data/HydroSHEDS'):
    """
    Description
    ----------
    create pickle files to more quickly read in HydroBASINS
    standard basin data in geodataframe, dataframe
    currently assumes set regions (see region variable)
    and file types (.shp) based on v1c standard basins

    Parameters
    ----------
    level: str, default '12'
        string representation of HydroBASINS PFAF levels
        acceptable values include
        ['02','03','04','05','06','07','08','09','10','11','12']
    version: str
        version of HydroBASINS as denoted in file_name

    Output
    ----------
    f'data/basins_lvl{level}_gdf.pkl': .pkl file
        pickle file of basin attributes for specified basin level with geometry
        for creating global gdf, read using def read_pkl_gdf
    f'data/basins_lvl{level}_df.pkl': .pkl file
        pickle file of basin attributes for specified basin level without
        geometry for creating global df, read using def read_pkl_df
    f'data/basins_lvl{level}.txt': .pkl file
        pickle file of full list of HydroBASIN ids for specified basin level,
        read using def read_pkl_df
    print : currently reports to user via print if wrong number of regional
        files detected, and if wrong number of identifiers is detected

    Notes
    ----------
    Explore switch to geofeather for geospatial file.
        write: to_geofeather(my_gdf, 'test.feather')
        read: from_geofeather('test.feather')
    Remove print / try where possible

    """
    regions = ['af', 'ar', 'as', 'au', 'eu', 'gr', 'na', 'sa', 'si']

    # ensure level is string
    level = str(level)

    # search HydroSHEDS data
    prefix = 'hybas_'
    suffix = f'_lev{level}_{version}.shp'
    files, directory = find_files(directory, prefix, suffix)

    # test to see if to many files are returned, kill process
    if len(files) > len(regions):
        print (f'There should be {str(len(regions))} files, there are {str(len(files))}')
        sys.exit()

    else:
        # test if not all regions using count
        # could be improved to return actual missing regions
        if len(files) < len(regions):
            n_missing = str(len(regions)-len(files))
            print (f'Missing {n_missing} regional files.')

        list_hybas_ids = []
        reg_df_list = []
        for file in files:
            file_path = file['file_path']
            try:
                regional_gdf = gpd.read_file(file_path)
            except:
                print (f'Failed to build geodataframe for: {file_path}')

            try:
                # add regional df to global list of dataframes
                regional_df = pd.DataFrame(regional_gdf)
                reg_df_list.append(regional_df)
                # Create list of ids in regional list and add to global list
                regional_id_list = (regional_df['HYBAS_ID']).to_list()
                list_hybas_ids += regional_id_list
            except:
                print (
                    f'Failed to convert to dataframe and list for: {file_path}'
                    )

        if len(list_hybas_ids) != 1034083:
            print (f'Warning, expected 1034083 ids, you have {str(len(list_hybas_ids))}')

        try:
            # pickle list of hydrobasin ids
            list_hybas_ids = list(set(list_hybas_ids))
            # print (len(list_hybas_ids))
            outfile_list_ids = f'data/basins_lvl{level}.txt'
            with open(outfile_list_ids, "wb") as f:
                pickle.dump(list_hybas_ids, f)

            # pickle dataframe of basin data for all regions
            basin_data_w_geom = pd.concat(reg_df_list)
            outfile_basin_gdf = f'data/basins_lvl{level}_gdf.pkl'
            basin_data_w_geom.to_pickle(outfile_basin_gdf)
            basin_data = basin_data_w_geom.drop(columns=['geometry'])
            outfile_basin_df = f'data/basins_lvl{level}_df.pkl'
            basin_data.to_pickle(outfile_basin_df)

        except:
            print ("Failed to pickle files")


def read_pkl_gdf(file_path='data/basins_lvl12_gdf.pkl',
                 crs={'init': 'epsg:4326'}
                 ):
    """Read pkl hydrobasin data into geodataframe."""
    temp_df = pd.read_pickle(file_path)
    gdf = gpd.GeoDataFrame(temp_df, crs=crs, geometry='geometry')
    return gdf


def read_pkl_df(file_path='data/basins_lvl12_df.pkl'):
    """Read pkl hydrobasin data to pandas dataframe."""
    df = pd.read_pickle(file_path)
    return df


def basin_list_by_pfaf_lvl(df, level=12):
    """Get unique list of PFAF Ids for hydrobasin level."""
    level = int(level)
    pfaf_lvl12_ids = df['PFAF_ID'].tolist()
    pfaf_lvlx_ids = list(set([str(x)[:level] for x in pfaf_lvl12_ids]))
    return pfaf_lvlx_ids
