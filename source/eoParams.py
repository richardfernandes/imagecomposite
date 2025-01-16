import eoImage as eoIM
import eoUtils as eoUs
import eoTileGrids as eoTG
from pathlib import Path

from datetime import datetime

#############################################################################################################
# Description: Define a default execution parameter dictionary. 
# 
# Revision history:  2022-Mar-29  Lixin Sun  Initial creation
#
#############################################################################################################
# DefaultParams = {
#     'sensor': 'S2_SR',           # A sensor type and data unit string (e.g., 'S2_Sr' or 'L8_SR')    
#     'unit': 2,                   # data unite (1=> TOA reflectance; 2=> surface reflectance)
#     'year': 2019,                # An integer representing image acquisition year
#     'nbYears': 1,                # positive int for annual product, or negative int for monthly product
#     'months': [5,6,7,8,9,10],    # A list of integers represening one or multiple monthes     
#     'tile_names': ['tile55'],    # A list of (sub-)tile names (defined using CCRS' tile griding system) 
#     'prod_names': ['mosaic'],    # ['mosaic', 'LAI', 'fCOVER', ]
#     'resolution': 30,            # Exporting spatial resolution
#     'out_folder': '',            # the folder name for exporting
#     'export_style': 'separate',
#     'start_date': '',
#     'end_date':  '',
#     'scene_ID': '',
#     'projection': 'EPSG:3979',
#     'CloudScore': False,

#     'current_month': -1,
#     'current_tile': '',
#     'time_str': '',              # Mainly for creating output filename
#     'region_str': ''             # Mainly for creating output filename
# }



all_param_keys = ['sensor', 'unit', 'year', 'nbYears', 'months', 'tile_names', 'prod_names', 
                  'out_location', 'resolution', 'GCS_bucket', 'out_folder', 'export_style', 'projection', 'CloudScore',
                  'monthly', 'start_dates', 'end_dates', 'regions', 'scene_ID', 'current_time', 'current_region', 'time_str', 'cloud_cover']




#############################################################################################################
# Description: This function tells if there is a customized region defined in parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#
#############################################################################################################
def has_custom_region(inParams):  
  nRegions = len(inParams['regions']) if 'regions' in inParams else 0
  
  if 'scene_ID' not in inParams:
    inParams['scene_ID'] = ''

  return True if nRegions > 0 or len(inParams['scene_ID']) > 5 else False 




#############################################################################################################
# Description: This function tells if customized time windows are defined in a given parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#
#############################################################################################################
def has_custom_window(inParams):
  start_len = len(inParams['start_dates']) if 'start_dates' in inParams else 0
  end_len   = len(inParams['end_dates']) if 'end_dates' in inParams else 0

  custom_time = False
  if start_len >= 1 and end_len >= 1 and start_len == end_len:
    custom_time = True
  
  elif start_len >= 1 and end_len >= 1 and start_len != end_len:  
    print('\n<has_custom_window> Inconsistent customized time list!')
  
  return custom_time
  




#############################################################################################################
# Description: This function sets values for 'current_month' and 'time_str' keys.
# 
# Note: If a customized time windows has been specified, then the given 'current_month' will be ignosed
#
# Revision history:  2024-Apr-08  Lixin Sun  Initial creation
#
#############################################################################################################
def set_current_time(inParams, current_time):
  '''Sets values for 'curent_time' and 'time_str' keys based on 'current_time' input
     Args:
       inParams(Dictionary): A dictionary storing required input parameters;
       current_time(Integer): An integer representing the index in the list corresponding to 'start_dates'/'end_dates' keys.'''
  
  if 'start_dates' not in inParams or 'end_dates' not in inParams:
    print('\n<set_current_time> There is no \'start_dates\' or \'end_dates\' key!')
    return None
  
  #==========================================================================================================
  # Ensure the given 'current_time' is valid.
  #==========================================================================================================
  ndates = len(inParams['start_dates'])

  if current_time < 0 or current_time >= ndates:
    print('\n<set_current_time> Invalid \'current_time\' was provided!')
    return None
  
  #==========================================================================================================
  # Set values for 'current_time' and 'time_str' keys
  #==========================================================================================================
  inParams['current_time'] = current_time

  if inParams['monthly']:
    inParams['time_str'] = eoIM.get_MonthName(int(inParams['months'][current_time]))
  else:  
    inParams['time_str'] = str(inParams['start_dates'][current_time]) + '_' + str(inParams['end_dates'][current_time])

  return inParams





#############################################################################################################
# Description: This function sets values for 'current_tile' and 'region_str' keys
# 
# Note: If a customized spatial region has been specified, then the given 'current_tile' will be ignosed
#
# Revision history:  2024-Apr-08  Lixin Sun  Initial creation
#
#############################################################################################################
def set_spatial_region(inParams, region_name):  
  if 'regions' not in inParams:
    print('\n<set_spatial_region> There is no \'regions\' key!')
    return None
  
  region_names = inParams['regions'].keys()

  if region_name not in region_names:
    print('<set_spatial_region> {} is an invalid tile name!'.format(region_name))
    return None
    
  inParams['current_region'] = region_name

  return inParams





#############################################################################################################
# Description: This function fills default values for some critical parameters
# 
# Revision history:  2024-Oct-08  Lixin Sun  Initial creation
#
#############################################################################################################
def fill_critical_params(inParams):  
  if 'sensor' not in inParams:
    inParams['sensor'] = 'S2_SR'

  if 'year' not in inParams:
    inParams['year'] = datetime.now().year

  if 'nbYears' not in inParams:
    inParams['nbYears'] = 1
  
  if 'prod_names' not in inParams:
    inParams['prod_names'] = []
  
  if 'out_location' not in inParams:
    inParams['out_location'] = 'drive'
 
  if 'resolution' not in inParams:
    inParams['resolution'] = 30
  
  if 'out_folder' not in inParams:
    inParams['out_folder'] = inParams['sensor'] + '_' + inParams['year'] + '_results' 

  if 'months' not in inParams:
    inParams['months'] = []

  if 'tile_names' not in inParams:
    inParams['tile_names'] = []

  if 'export_style' not in inParams:
    inParams['export_style'] = 'separate'

  if 'projection' not in inParams:
    inParams['projection'] = 'EPSG:3979'
  
  sensor_type = inParams['sensor'].lower()
  if sensor_type.find('s2') < 0:
    inParams['CloudScore'] = False 

  return inParams




#############################################################################################################
# Description: This function validate a given user parameter dictionary.
#
# Revision history:  2024-Jun-07  Lixin Sun  Initial creation
#       
#############################################################################################################
def valid_user_params(UserParams):
  #==========================================================================================================
  # Ensure all the keys in user's parameter dictionary are valid
  #==========================================================================================================
  all_valid    = True
  user_keys    = list(UserParams.keys())
  default_keys = all_param_keys
  n_user_keys  = len(user_keys)

  key_presence = [element in default_keys for element in user_keys]
  for index, pres in enumerate(key_presence):
    if pres == False and index < n_user_keys:
      all_valid = False
      print('<valid_user_params> \'{}\' key in given parameter dictionary is invalid!'.format(user_keys[index]))
  
  if not all_valid:
    return all_valid, None
  
  #==========================================================================================================
  # Fill default values for some critical parameters as necessary
  #==========================================================================================================
  out_Params = fill_critical_params(UserParams)

  #==========================================================================================================
  # Validate values of critical parameters
  #==========================================================================================================
  sensor_name = str(out_Params['sensor']).upper()
  all_SSRs = ['S2_SR', 'L5_SR', 'L7_SR', 'L8_SR', 'L9_SR']
  if sensor_name not in all_SSRs:
    all_valid = False
    print('<valid_user_params> Invalid sensor or unit was specified!')

  year = int(out_Params['year'])
  if year < 1970 or year > datetime.now().year:
    all_valid = False
    print('<valid_user_params> Invalid year was specified!')

  nYears = int(out_Params['nbYears'])
  if nYears > 3:
    all_valid = False
    print('<valid_user_params> Invalid number of years was specified!')

  prod_names = out_Params['prod_names']
  nProds = len(prod_names)
  if nProds < 1:
    all_valid = False
    print('<valid_user_params> No product name was specified for prod_names key!')
  
  valid_prod_names = ['LAI', 'fAPAR', 'fCOVER', 'Albedo', 'mosaic', 'QC', 'date', 'partition']
  presence = [element in valid_prod_names for element in prod_names]
  if False in presence:
    all_valid = False
    print('<valid_user_params> At least one of the specified products is invalid!')
  
  valid_out_locations = ['DRIVE', 'STORAGE', 'ASSET']
  out_location = str(out_Params['out_location']).upper()  
  if out_location not in valid_out_locations:
    all_valid = False
    print('<valid_user_params> Invalid out location was specified!')

  resolution = int(out_Params['resolution'])
  if resolution < 1:
    all_valid = False
    print('<valid_user_params> Invalid spatial resolution was specified!')

  out_folder = str(out_Params['out_folder'])
  if Path(out_folder) == False or len(out_folder) < 2:
    all_valid = False
    print('<valid_user_params> The specified output path is invalid!')

  if out_Params['months'] is not None:
    max_month = max(out_Params['months'])
    if max_month > 12:
      all_valid = False
      print('<valid_user_params> Invalid month number was specified!')

  tile_names = out_Params['tile_names']
  nTiles = len(tile_names)
  if nTiles < 1:
    all_valid = False
    print('<valid_user_params> No tile name was specified for tile_names key!')
  
  for tile in tile_names:
    if eoTG.valid_tile_name(tile) == False:
      all_valid = False
      print('<valid_user_params> {} is an invalid tile name!'.format(tile))
  
  return all_valid, out_Params




#############################################################################################################
# Description: This function creates the start and end dates for a list of user-specified months and save 
#              them into two lists with 'start_dates' and 'end_dates' keys.
#
# Revision history  2024-Sep-03  Lixin Sun  Initial creation
#
#############################################################################################################
def form_time_windows(inParams):
  if not has_custom_window(inParams):
    inParams['monthly'] = True
    nMonths = len(inParams['months'])  # get the number of specified months

    year = inParams['year']
    for index in range(nMonths):
      month = inParams['months'][index]
      start, end = eoUs.month_range(year, month)

      if index == 0:
        inParams['start_dates'] = [start]
        inParams['end_dates']   = [end]
      else:  
        inParams['start_dates'].append(start)
        inParams['end_dates'].append(end) 

  else:
    inParams['monthly'] = False
    if inParams['months'] is None:
      start = datetime.strptime(inParams['start_dates'][0], '%Y-%m-%d')
      end = datetime.strptime(inParams['end_dates'][0], '%Y-%m-%d')
    
      # Extract the start and end months
      start_month = start.month
      end_month = end.month
      
      if start.year == end.year and start_month == end_month:
        inParams['months'] = [start_month]
      
      # If the dates span across multiple months, generate a list of months
      months = []
      if start.year == end.year:
        # Same year, just need to get the range of months
        for month in range(start_month, end_month + 1):
            months.append(month)
        inParams['months'] = months
      else:
        # Different years, get months from start month to December of start year
        for month in range(start_month, 13):
            months.append(month)
        # Get months from January of end year to end month
        for month in range(1, end_month + 1):
            months.append(month)
        inParams['months'] = months
    max_month = max(inParams['months'])
    if max_month > 12:
      all_valid = False
      print('<valid_user_params> Invalid month number was specified!')

  return set_current_time(inParams, 0)
  




#############################################################################################################
# Description: This function creates the start and end dates for a list of user-specified months and save 
#              them into two lists with 'start_dates' and 'end_dates' keys.
#
# Revision history  2024-Sep-03  Lixin Sun  Initial creation
#
#############################################################################################################
def form_spatial_regions(inParams):
  if not has_custom_region(inParams):
    inParams['regions'] = {}
    for tile_name in inParams['tile_names']:      
      if eoTG.valid_tile_name(tile_name):
        inParams['regions'][tile_name] = eoTG.get_tile_polygon(tile_name)
    
    return set_spatial_region(inParams, inParams['tile_names'][0])  
  
  else:
    return inParams




#############################################################################################################
# Description: This function modifies default parameter dictionary based on a given parameter dictionary.
# 
# Note:        The given parameetr dictionary does not have to include all "key:value" pairs, only the pairs
#              as needed.
#
# Revision history:  2022-Mar-29  Lixin Sun  Initial creation
#                    2024-Apr-08  Lixin Sun  Incorporated modifications according to customized time window
#                                            and spatial region.
#                    2024-Sep-03  Lixin Sun  Adjusted to ensure that regular months/season will also be 
#                                            handled as customized time windows.    
#############################################################################################################
def update_default_params(inParams):  
  all_valid, out_Params = valid_user_params(inParams)

  if all_valid == False or out_Params == None:
    return None

  #==========================================================================================================
  # If regular months (e.g., 5,6,7) or season (e.g., -1) are specified, then convert them to date strings and
  # save in the lists corresponding to 'start_dates' and 'end_dates' keys. In this way, regular months/season
  # will be dealed with as customized time windows.    
  #==========================================================================================================
  out_Params = form_time_windows(out_Params)  
 
  #==========================================================================================================
  # If only regular tile names are specified, then create a dictionary with tile names and their 
  # corresponding 'ee.Geometry.Polygon' objects as keys and values, respectively.   
  #==========================================================================================================
  out_Params = form_spatial_regions(out_Params)  
 
  # return modified parameter dictionary 
  return out_Params





############################################################################################################# 
# Description: Obtain a parameter dictionary for LEAF tool
#############################################################################################################
def get_LEAF_params(inParams):
  out_Params = update_default_params(inParams)  # Modify default parameters with given ones  
  out_Params['unit'] = 2                     # Always surface reflectance for LEAF production
  
  return out_Params  




#############################################################################################################
# Description: Obtain a parameter dictionary for Mosaic tool
#############################################################################################################
def get_mosaic_params(inParams):
  out_Params = update_default_params(inParams)  # Modify default parameter dictionary with a given one
  out_Params['prod_names'] = ['mosaic']         # Of course, product name should be always 'mosaic'

  return out_Params  




#############################################################################################################
# Description: Obtain a parameter dictionary for land cover classification tool
#############################################################################################################
def get_LC_params(inParams):
  out_Params = update_default_params(inParams) # Modify default parameter dictionary with a given one
  out_Params['prod_names'] = ['mosaic']        # Of course, product name should be always 'mosaic'

  return out_Params 


#############################################################################################################
# Description: Obtain the cloud cover from input dictionary
#############################################################################################################
def get_cloud_coverage(inParams):

  return inParams['cloud_cover']


#############################################################################################################
# Description: This function returns a valid spatial region defined in parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#
#############################################################################################################
def get_spatial_region(inParams):
  if 'current_region' not in inParams or 'regions' not in inParams:
    print('\n<get_spatial_region> one of required keys is not exist!!')
    return None

  reg_name        = inParams['current_region']
  valid_reg_names = inParams['regions'].keys()

  if reg_name in valid_reg_names:
    return inParams['regions'][reg_name]
  else:
    print('\n<get_spatial_region> Invalid spatial region name provided!')
    return None





#############################################################################################################
# Description: This function returns a valid time window defined in parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#                    2024-Sep-03  Lixin Sun  Added 'AutoIncr' input parameter so that the value 
#                                            corresponding to 'current_time' is increased automatically. 
#############################################################################################################
def get_time_window(inParams):
  if 'current_time' not in inParams or 'start_dates' not in inParams or 'end_dates' not in inParams:
    print('\n<get_time_window> one of required keys is not exist!!')
    return None, None

  current_time = inParams['current_time']
  nDates       = len(inParams['start_dates'])
    
  if current_time >= nDates:
    print('\n<get_time_window> Invalidate \'current_time\' value!')
    return None, None

  start = inParams['start_dates'][current_time]
  end   = inParams['end_dates'][current_time]
  
  return start, end


