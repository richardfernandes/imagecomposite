import os
import time
import numpy as np
import pandas as pd
import xarray as xr
#import odc.stac

from datetime import datetime
import concurrent.futures

import eoImage as eoIM
import eoMosaic as eoMz
import eoUtils as eoUs
import eoParams as eoPM
import eoTileGrids as eoTG
import eoAuxData as eoAD

import SL2P_V1
import SL2P_NetsTools





#############################################################################################################
# Description: This function produces all the required vegetation parameter maps for ONE tile and ONE month.
# 
# Note: This function is equivalent to the 'SL2P_estimation' in GEE LEAF package
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-20  Lixin Sun  Modified to generate the final composite image tile by tile.
#
#############################################################################################################
def create_LEAF_maps(inParams):
  '''
    Args:
      inParams(dictionary): A dictionary containing all execution parameters.'''

  leaf_start = time.time()
  #==========================================================================================================
  # Validate input parameters
  #==========================================================================================================
  if len(inParams['current_region']) < 6: #Ensure the existence of a valid 'current_region' item
    print('<create_LEAF_maps> Invalid <current_tile> item in parameter dictionary!')
    return None
  
  #==========================================================================================================
  # Get a list of unique granule names
  #==========================================================================================================
  mosaic = eoMz.period_mosaic(inParams, eoIM.EXTRA_ANGLE)
  print('\n<create_LEAF_maps> The bands in mosaic image:', mosaic.data_vars)

  #==========================================================================================================
  # (1) Read and clip land cover map based on the spatial extent of "entire_map"
  # (2) Create a network ID map with the same spatial dimensions as clipped landcover map
  #==========================================================================================================  
  #sub_LC_map = eoAD.get_local_CanLC('F:\\Canada_LC2020\\Canada_LC_2020_30m.tif', entire_map) # for workstation at Observatory
  sub_LC_map = eoAD.get_local_CanLC('C:\\Work_documents\\Canada_LC_2020_30m.tif', mosaic) # for work laptop
  SsrData    = eoIM.SSR_META_DICT[str(inParams['sensor']).upper()]

  DS_Options = SL2P_V1.make_DS_options('sl2p_nets', SsrData)  
  netID_map  = SL2P_NetsTools.makeIndexLayer(sub_LC_map, DS_Options)

  #==========================================================================================================
  # Define a function that can produce vegetation parameter maps for ONE granule
  #==========================================================================================================
  ready_mosaic = eoIM.rescale_spec_bands(mosaic, SsrData['LEAF_BANDS'], 0.01, 0)
  out_VP_maps  = SL2P_NetsTools.estimate_VParams(inParams, DS_Options, ready_mosaic, netID_map)  
    
  #==========================================================================================================
  # Display the elapsed time for entire process
  #==========================================================================================================
  leaf_stop = time.time()
  leaf_time = (leaf_stop - leaf_start)/60
  print('\n\n<<< The elapsed time for generating one monthly tile product = %6.2f minutes>>>'%(leaf_time))

  return out_VP_maps





#############################################################################################################
# Description: Produces LEAF products for a customized spatial region and time window or a specified scene
# 
# Note: This function will be called when one of the following three situations happens:
#       (1) A ee.Geometry.Polygon object is provided as the value corresponding to "custom_region" key
#       (2) A user-specified scene ID is provided as the value corresponding to "scene_ID" key
#       (3) A time window is provided as the values corresponding to "start_date" and "end_date" keys
#
# Revision history:  2023-Nov-26  Lixin Sun  Initial creation 
#
#############################################################################################################
def SL2P_estimation(Params):
  '''Produces LEAF products for one or multiple tiles in CANADA

    Args:
      Params(Dictionary): A dictionary containing all execution input parameters.'''  
  
  #==========================================================================================================
  # Obtain some required parameters
  #==========================================================================================================
  SsrData     = eoIM.SSR_META_DICT[Params['sensor']]
  year        = int(Params['year'])
  SceneID     = str(Params['scene_ID'])    # An optional ID of a single scene/granule 
  ProductList = Params['prod_names']       # A list of products to be generated
  tile_name   = str(Params['current_tile'])

  #==========================================================================================================
  # Obtain timeframe and spatial region
  #==========================================================================================================
  start, stop = eoPM.get_time_window(Params)
  region      = eoPM.get_spatial_region(Params)
  if len(tile_name) > 2:
     region = eoTG.expandSquare(region, 0.02)  

  #print('<apply_SL2P> All parameters:', Params) 

  #==========================================================================================================
  # Obtain a global Land cover classification map and export it as needed 
  #==========================================================================================================
  ClassImg = eoAD.get_GlobLC(year, False).uint8().clip(region)

  #==========================================================================================================
  # If scene_ID is provided, ontain its footprint as ROI
  #==========================================================================================================
  if len(SceneID) > 5: 
    # Obtain the specified single scene and its footprint
    ssr_code, tile_str, refer_date_str, valid_ID = eoIM.parse_ImgID(SceneID)  # parse the given image ID string
    '''
    if valid_ID == True and SsrData['SSR_CODE'] == ssr_code:
      image  = ee.Image(SsrData['GEE_NAME'] + '/' + SceneID) 
      image  = eoIM.apply_gain_offset(image, SsrData, 1, False)  # convert SR to range between 0 and 1
      image  = eoIM.attach_AngleBands(image, SsrData)            # attach three imaging angle bands
      region = ee.Image(image).geometry()
      
      SL2P_separate_params(Params, image, region, SsrData, ClassImg)
    '''
  else: 
    mosaic = Mosaic.LEAF_Mosaic(SsrData, region, start, stop, True)   
    print("<apply_SL2P> The band names in mosiac image = ", mosaic.bandNames().getInfo())

    SL2P_separate_params(Params, mosaic, region, SsrData, ClassImg)

    if Is_export_required('date', ProductList):
      Params['prod_name'] = 'Date'
      date_map = mosaic.select([eoIM.pix_date])
      export_one_map(Params, region, date_map, 'Date')        




#############################################################################################################
# Description: This function exports the vegetation parameter maps into separate GeoTiff files
#
# Revision history:  2024-Aug-15  Lixin Sun  Initial creation
# 
#############################################################################################################
def export_VegParamMaps(inParams, inXrDS):
  '''
    This function exports the band images of a mosaic into separate GeoTiff files.

    Args:
      inParams(dictionary): A dictionary containing all required execution parameters;
      inXrDS(xrDS): A xarray dataset object containing mosaic images to be exported.'''
  
  #==========================================================================================================
  #
  #==========================================================================================================  
  VP_scalers = {}
  for s in inXrDS.data_vars:
    S = s.upper()
    if 'LAI' in S:
      VPOptions = SL2P_V1.make_VP_options('lai')
      VP_scalers[s] = VPOptions['scale_factor']
    elif 'FAPAR' in S or 'FCOVER' in S or 'ALBEDO' in S:  
      VPOptions = SL2P_V1.make_VP_options('FAPAR')
      VP_scalers[s] = VPOptions['scale_factor']
    else:
      VP_scalers[s] = 1

  #==========================================================================================================
  # Apply projection
  #==========================================================================================================
  rio_xrDS = inXrDS.rio.write_crs(inParams['projection'], inplace=True)  # Assuming WGS84 for this example

  #==========================================================================================================
  # Create a directory to store the output files
  #==========================================================================================================
  dir_path = inParams['out_folder']
  os.makedirs(dir_path, exist_ok=True)

  #==========================================================================================================
  # Create prefix filename
  #==========================================================================================================
  SsrData    = eoIM.SSR_META_DICT[str(inParams['sensor'])]   
  region_str = str(inParams['current_region'])
  period_str = str(inParams['time_str'])
 
  filePrefix = f"{SsrData['NAME']}_{region_str}_{period_str}"

  #==========================================================================================================
  # Create individual sub-mosaic and combine it into base image based on score
  #==========================================================================================================
  spa_scale    = inParams['resolution']
  export_style = str(inParams['export_style']).lower()
  
  if 'sepa' in export_style:
    for band in rio_xrDS.data_vars:
      out_img     = (rio_xrDS[band]*VP_scalers[band]).astype(np.uint8)
      filename    = f"{filePrefix}_{band}_{spa_scale}m.tif"
      output_path = os.path.join(dir_path, filename)
      out_img.rio.to_raster(output_path)
  else:
    filename = f"{filePrefix}_LEAF_{spa_scale}m.tif"

    output_path = os.path.join(dir_path, filename)
    rio_xrDS.to_netcdf(output_path)




#############################################################################################################
# Description: This function can be used to produce monthly vegetation parameter maps for one or multiple 
#              tiles within Canada.
# 
# Revision history:  2024-Aug-01  Lixin Sun  Initial creation 
#
#############################################################################################################
def tile_LEAF_production(Params):
  '''Produces monthly vegetation parameter maps for one or multiple tiles within Canada.

    Args:
       Params(dictionary): A dictionary containing all necessary execution parameters.'''  
  
  #==========================================================================================================
  # Validate some input parameters
  #==========================================================================================================
  all_keys  = Params.keys()
  all_valid = True if 'tile_names' in all_keys and 'months' in all_keys and 'year' in all_keys else False
  
  if all_valid == False:
    print('<tile_LEAF_production> !!!!!!!! Required parameters are not available !!!!!!!!')
    return 

  #==========================================================================================================
  # Produce vegetation parameter maps for eath tile and each month
  #==========================================================================================================
  # Loop for each tile
  for tile in Params['tile_names']:    
    Params['current_tile'] = tile   
    Params['region_str']   = tile   # Update region string

    # Produce monthly (or seasonal) porducts 
    for month in Params['months']:
      # Add an element with 'month' as key to 'exe_param_dict'  
      Params['current_month'] = month
      Params['time_str']      = eoIM.get_MonthName(int(month))

      # Produce monthly/seasonal vegetation parameter maps and export them in a specified way
      # (a compact image or separate images)      
      out_style = str(Params['export_style']).lower()
      if out_style.find('comp') > -1:
        print('\n<tile_LEAF_production> Generate and export biophysical maps in one file .......')
        #out_params = compact_params(mosaic, SsrData, ClassImg)

        # Export the 64-bits image to either GD or GCS
        #export_compact_params(fun_Param_dict, region, out_params, task_list)

      else: 
        # Produce and export monthly/seasonal vegetation biophysical parameetr (VBP) maps
        print('\n<tile_LEAF_production> Generate and export separate vegetation biophysical maps......')        
        VBP_maps = create_LEAF_maps(Params)
      
        # Export results for ONE tile and ONE month/season        
        export_VegParamMaps(Params, VBP_maps)






#############################################################################################################
# Description: This is the main fuction that can produce vegetation parameter maps according to a given
#              execution parameter dictionary.
# 
# Revision history:  2024-Jul-30  Lixin Sun  Initial creation
#
#############################################################################################################
def LEAF_production(inExeParams):
  '''Produces monthly biophysical parameter maps for a number of tiles and months.

     Args:
       inExeParams(Python Dictionary): A Python dictionary containing all input parameters for one execution.'''

  #==========================================================================================================
  # Standardize the execution parameters so that they are applicable for producing vegetation parameter maps
  #==========================================================================================================
  Params = eoPM.get_LEAF_params(inExeParams)
  print('<LEAF_production> All input parameters = ', Params) 

  #==========================================================================================================
  # Produce vegetation parameter porducts for eath region and each time window
  #==========================================================================================================
  region_names = Params['regions'].keys()
  nTimes       = len(Params['start_dates'])

  for reg_name in region_names:
    Params = eoPM.set_spatial_region(Params, reg_name)

    # Produce vegetation parameter porducts for each time window
    for TIndex in range(nTimes):
      Params = eoPM.set_current_time(Params, TIndex)

      # Produce and export products in a specified way (a compact image or separate images)      
      out_style = str(Params['export_style']).lower()
      if out_style.find('comp') > -1:
        print('\n<LEAF_production> Generate and export biophysical maps in one file .......')
        #out_params = compact_params(mosaic, SsrData, ClassImg)

        # Export the 64-bits image to either GD or GCS
        #export_compact_params(fun_Param_dict, region, out_params, task_list)

      else: 
        # Produce and export vegetation parameetr maps for a time period and a region
        print('\n<tile_LEAF_production> Generate and export separate vegetation biophysical maps......')        
        VBP_maps = create_LEAF_maps(Params)
      
        # Export results for ONE tile and ONE month/season        
        export_VegParamMaps(Params, VBP_maps)   

    



# params = {
#     'sensor': 'S2_SR',           # A sensor type string (e.g., 'S2_SR' or 'L8_SR' or 'MOD_SR')
#     'unit': 2,                   # A data unit code (1 or 2 for TOA or surface reflectance)    
#     'year': 2023,                # An integer representing image acquisition year
#     'nbYears': -1,               # positive int for annual product, or negative int for monthly product
#     'months': [7,8],               # A list of integers represening one or multiple monthes     
#     'tile_names': ['tile55_921', 'tile55_922'], # A list of (sub-)tile names (defined using CCRS' tile griding system) 
#     'prod_names': ['LAI', 'fCOVER', 'fAPAR', 'Albedo'],    #['mosaic', 'LAI', 'fCOVER', ]    
#     'resolution': 200,            # Exporting spatial resolution    
#     'out_folder': 'C:/Work_documents/LEAF_tile55_2023_922_Aug_200m_new',  # the folder name for exporting
#     'projection': 'EPSG:3979',    
#     #'start_date': '2022-06-15',
#     #'end_date': '2022-09-15'
# }

# LEAF_production(params)