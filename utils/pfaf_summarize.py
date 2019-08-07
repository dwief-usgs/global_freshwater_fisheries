#Import packages
import pandas as pd 
import json
from functools import reduce


############################################################################################
############################################################################################
'''
    Author
    ---------
    Daniel Wieferich: dwieferich@usgs.gov

    Description
    ---------
    module that summarizes information up the pfaf system in hydrobasins
'''
############################################################################################
############################################################################################

def hb12_json_to_csv(json_file):
    '''
    Description
    ---------
    Transforms json data from attribution step to csv.  
    (This function is currently specific to HydroBASIN level 12 attribution)
    This drops some src data from the json file and only carries along summary data.

    Parameters
    ---------
    json_file : str representing dir, file name, and extension (e.g. 'output/as_hb12_pop_stats.json')

    Output
    ---------
    csv file : with pfaf_12 identifier and one column for each attributed value (i.e. mean, max, min, sum, nodata, count)
    hb12_df : pandas dataframe with information that was exported to csv
    '''

    #Open json file
    with open(json_file, "r") as stats:
        data = json.load(stats)

    #Loop through records, recording dictionaries of data we are interested in, create list of dictionaries
    list_info = []
    for record in data:
        info = {}
        #record pfaf identifier
        info['pfaf_12'] = record['pfaf_12']
        #record area of hydrobasin level 12
        info['sub_area'] = record['sub_area']
        for stat in record:
            #grab all stats, avoiding identifiers and area fields
            #Note may want to change json structure to make these types of queries more intuitive?
            if stat != 'id' and stat != 'pfaf_12' and stat != 'sub_area':
                # for each stat grab column name = key and value = value
                for key, value in record[stat].items():
                    if key.endswith(('mean','max','min','sum','nodata','count')) and value is not None:
                        info[key] = value
        list_info.append(info) 

    #Create pandas dataframe to return to user and for ease of export to csv
    hb12_df = pd.DataFrame(list_info)
    
    #use json file name (input parameter) to name csv
    outfile_name = json_file.replace('.json','.csv')
    hb12_df.to_csv(outfile_name, sep=',')

    return hb12_df

    
    
def pfaf_summary_prep(json_file):
    '''
    Description
    ---------
    Transforms json data from attribution step to csv. 
    Similar to def hb12_json_to_csv(json_file) except mean values are preped for area weighted summaries by multiply values by area

    Parameters
    ---------
    json_file : str representing dir, file name, and extension (e.g. 'output/as_hb12_pop_stats.json')

    Output
    ---------
    hb12_df : pandas dataframe
    '''

    #Open json file
    with open(json_file, "r") as stats:
        data = json.load(stats)
    
    #Loop through records, recording dictionaries of data we are interested in, create list of dictionaries
    list_info = []
    for record in data:
        info = {}
        #record pfaf identifier
        info['pfaf_12'] = record['pfaf_12']
        #record area of hydrobasin level 12
        info['sub_area'] = record['sub_area']
        for stat in record:
            #grab all stats, avoiding identifiers and area fields
            if stat != 'id' and stat != 'pfaf_12' and stat != 'sub_area':
                for key, value in record[stat].items():
                    #for mean stats prep for pfaf area weighted summaries by mult val with area
                    if key.endswith('mean') and value is not None:
                        key_nm = f'{key}_area_mean'
                        info[key_nm] = value * record['sub_area']
                    # all other stats val = val
                    elif key.endswith(('max','min','sum','nodata','count')) and value is not None:
                        info[key] = value
        list_info.append(info) 
    
    #Create pandas dataframe and return  
    att12_df = pd.DataFrame(list_info)
    return att12_df


def summarize(att12_df, pfaf_level):
    '''
    Description
    ---------
    
    Note all rows that have any nan data will be dropped so its important to run this separate for each src
    
    Parameters
    ---------
    att12_df : pandas dataframe from pfaf_summary_prep
    pfaf_level: HydroBASINS pfaf basin level, str, choices include ['02','03','04','05','06','07','08','09','10','11']

    Output
    ---------
    df_final : pandas dataframe with summaries of each stat for given pfaf value
    '''

    
    #pfaf level
    pfaf_field_name = f'pfaf_{pfaf_level}'
    ##grab left x digits of pfaf_12 for x level of pfaf, store as int
    att12_df[pfaf_field_name]= (att12_df['pfaf_12'].str[0:int(pfaf_level)]).astype(int)

    #Create lists of column names for each type of statistic that we are handling
    #Note sum, count and nodata will require a sum as summary so all clumped together
    column_names = att12_df.columns
    mean_columns = [col for col in column_names if col.endswith(('mean'))]
    max_columns = [col for col in column_names if col.endswith(('max',pfaf_field_name))] 
    min_columns = [col for col in column_names if col.endswith(('min',pfaf_field_name))] 
    sum_columns = [col for col in column_names if col.endswith(('sum','count','nodata',pfaf_field_name))] 

    #Create seperate df with data for each type of summary, add to a list of dataframes for merge at end
    list_dfs = []
    #Deal with area-weighting of mean stats, mean columns are init vals* area so we just need to sum those and divide by sum of area for each basin
    #Note mean dataframe will always be captured... if no mean stats it will at least sum the area
    mean_all_columns = list(set(mean_columns + [pfaf_field_name,'sub_area']))
    mean_df = att12_df[mean_all_columns]
    #sums data for each level 12 basin in target pfaf level basin
    mean_df = mean_df.groupby([pfaf_field_name]).sum()
    mean_df.reset_index(inplace=True)
    for column in mean_columns:
        #Next two lines give us a more appropriate name for the stat
        suffix = '_area_mean'
        column_name = column[:(len(column)-len(suffix))]
        #Calculate the summary, add to dataframe
        mean_df[column_name] = mean_df[column]/mean_df['sub_area']
    #drop temporary fields that we do not want in dataframe
    mean_df = mean_df.drop(columns=mean_columns)
    mean_df.reset_index
    #Append mean dataframe to list of dataframes
    list_dfs.append(mean_df)        
    
    #Summarize max stats, taking max of all values
    if len(max_columns) >0:
        max_df = att12_df[max_columns]
        max_df = max_df.groupby([pfaf_field_name]).max()
        max_df.reset_index(inplace=True)
        list_dfs.append(max_df)  
    
    #Summarize min stats, taking min of all values
    if len(min_columns) >0:
        min_df = att12_df[min_columns]
        min_df = min_df.groupby([pfaf_field_name]).min()
        min_df.reset_index(inplace=True)
        list_dfs.append(min_df)  
    
    #Summarize sum stats, taking sum of all values
    if len(sum_columns) >0:
        sum_df = att12_df[sum_columns]
        sum_df = sum_df.groupby([pfaf_field_name]).sum()
        sum_df.reset_index(inplace=True)
        list_dfs.append(sum_df)  

    #Combine dataframes using the pfaf id field
    if len(list_dfs)>1:
        df_final = reduce(lambda left,right: pd.merge(left,right,on=pfaf_field_name), list_dfs)
    else:
        #If only 1 dataframe it must be the mean dataframe (with at least sum of area)
        df_final = mean_df

    #Return final dataframe of all summarized data
    return df_final


############################################################################################
############################################################################################