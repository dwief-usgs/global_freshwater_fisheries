#Import packages
import os

############################################################################################
############################################################################################
'''
    Author
    ---------
    Daniel Wieferich: dwieferich@usgs.gov

    Description
    ---------
    finds files of a specific type, used to develop config json

    Parameters
    ---------
    dir = directory to search (str)
    file_ext = file extension to search (str) (e.g. '.tif')
    
'''

def find_file_type(dir, file_ext):
    file_list = []
    for root, dirs, files in os.walk(dir):
        file_list += [ (os.path.join(root, file)) for file in files if file.endswith(file_ext)]
    
    return file_list



############################################################################################
############################################################################################