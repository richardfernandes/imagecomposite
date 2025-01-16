import gc
import os
import sys
import dask
import math
import time
import uuid
import shutil
import logging
import odc.stac
import requests
import warnings
import functools
import numpy as np
import xarray as xr
import pandas as pd
import dask.array as da
from dask import delayed
from pathlib import Path
import concurrent.futures
import pystac_client as psc
from datetime import datetime
import dask.diagnostics as ddiag
import xml.etree.ElementTree as ET
from collections import defaultdict
from dask.distributed import as_completed
from urllib3.exceptions import TimeoutError, ConnectionError
odc.stac.configure_rio(cloud_defaults = True, GDAL_HTTP_UNSAFESSL = 'YES')
dask.config.set(**{'array.slicing.split_large_chunks': True})

# The two things must be noted:
# (1) this line must be used after "import odc.stac"
# (2) This line is necessary for exporting a xarray dataset object into separate GeoTiff files,
#     even it is not utilized directly
import rioxarray
if str(Path(__file__).parents[0]) not in sys.path:
  sys.path.insert(0, str(Path(__file__).parents[0]))
import eoImage as eoIM
import eoUtils as eoUs
import eoParams as eoPM

logging.basicConfig(level=logging.WARNING) 

def get_query_conditions(SsrData, StartStr, EndStr, ClCover):
  ssr_code = SsrData['SSR_CODE']
  query_conds = {}
  
  #==================================================================================================
  # Create a filter for the search based on metadata. The filtering params will depend upon the 
  # image collection we are using. e.g. in case of Sentine 2 L2A, we can use params such as: 
  #
  # eo:cloud_cover
  # s2:dark_features_percentage
  # s2:cloud_shadow_percentage
  # s2:vegetation_percentage
  # s2:water_percentage
  # s2:not_vegetated_percentage
  # s2:snow_ice_percentage, etc.
  # 
  # For many other collections, the Microsoft Planetary Computer has a STAC server at 
  # https://planetarycomputer-staging.microsoft.com/api/stac/v1 (this info comes from 
  # https://www.matecdev.com/posts/landsat-sentinel-aws-s3-python.html)
  #==================================================================================================  
  if ssr_code > eoIM.MAX_LS_CODE and ssr_code < eoIM.MOD_sensor:
    query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
    query_conds['collection'] = "sentinel-2-l2a"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    query_conds['bands']      = SsrData['ALL_BANDS'] + ['scl']
    query_conds['filters']    = {"eo:cloud_cover": {"lt": ClCover} }    

  elif ssr_code < eoIM.MAX_LS_CODE and ssr_code > 0:
    #query_conds['catalog']    = "https://landsatlook.usgs.gov/stac-server"
    #query_conds['collection'] = "landsat-c2l2-sr"
    query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
    query_conds['collection'] = "landsat-c2-l2"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    #query_conds['bands']      = ['OLI_B2', 'OLI_B3', 'OLI_B4', 'OLI_B5', 'OLI_B6', 'OLI_B7', 'qa_pixel']
    query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel']
    query_conds['filters']    = {"eo:cloud_cover": {"lt": 85.0}}  
  elif ssr_code == eoIM.HLS_sensor:
    query_conds['catalog']    = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD"
    query_conds['collection'] = "HLSL30.v2.0"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel']
    query_conds['filters']    = {"eo:cloud_cover": {"lt": 85.0}}  

  return query_conds

#############################################################################################################
# Description: This function returns average view angles (VZA and VAA) for a given STAC item/scene
#
# Revision history:  2024-Jul-23  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_View_angles(StacItem):
  # Create a custom adapter with retry settings
  # session = requests.Session()
  # retry_strategy = requests.adapters.Retry(total=3, backoff_factor=1)
  # session.mount('http://', retry_strategy)

  '''StacItem: a item obtained from the STAC catalog at AWS'''
  assets = dict(StacItem.assets.items())
  granule_meta = assets['granule_metadata']

  view_angles = {}
  try:
    response = requests.get(granule_meta.href)
    response.raise_for_status()  # Check that the request was successful

    # Parse the XML content
    root = ET.fromstring(response.content)  
    
    elem = root.find(".//Mean_Viewing_Incidence_Angle[@bandId='8']")
    view_angles['vza'] = float(elem.find('ZENITH_ANGLE').text)
    view_angles['vaa'] = float(elem.find('AZIMUTH_ANGLE').text)

  except Exception as e:
    view_angles['vza'] = 0.0
    view_angles['vaa'] = 0.0
  
  return view_angles
   

def display_meta_assets(stac_items, First):
  if First == True:
    first_item = stac_items[0]

    print('<<<<<<< The assets associated with an item >>>>>>>\n' )
    for asset_key, asset in first_item.assets.items():
      #print(f"Band: {asset_key}, Description: {asset.title or 'No title'}")
      print(f"Asset key: {asset_key}, title: {asset.title}, href: {asset.href}")    

    print('<<<<<<< The meta data associated with an item >>>>>>>\n' )
    print("ID:", first_item.id)
    print("Geometry:", first_item.geometry)
    print("Bounding Box:", first_item.bbox)
    print("Datetime:", first_item.datetime)
    print("Properties:")

    for key, value in first_item.properties.items():
      print(f"  <{key}>: {value}")
  else:
    for item in stac_items:
      properties = item.properties
      print("ID: {}; vza: {}; vaa: {}".format(item.id, properties['vza'], properties['vaa']))


#############################################################################################################
# Description: This function returns the results of searching a STAC catalog
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-12  Lixin Sun  Added a filter to retain only one image from the items with
#                                            identical timestamps.
#
#############################################################################################################
def search_STAC_Catalog(Region, Criteria, MaxImgs, ExtraBandCode):
  '''
    Args:
      Region(): A spatial region;

  '''
  #==========================================================================================================
  # use publically available stac 
  #==========================================================================================================
  catalog = psc.client.Client.open(str(Criteria['catalog'])) 

  #==========================================================================================================
  # Search and filter a image collection
  #==========================================================================================================
  print('<search_STAC_Images> The given region = ', Region)
  stac_catalog = catalog.search(collections = [str(Criteria['collection'])], 
                                intersects  = Region,                           
                                datetime    = str(Criteria['timeframe']), 
                                query       = Criteria['filters'],
                                limit       = MaxImgs)        
    
  stac_items = list(stac_catalog.items())
    
  #==========================================================================================================
  # Ingest imaging geometry angles into each STAC item
  #==========================================================================================================  
  stac_items, angle_time = ingest_Geo_Angles(stac_items, ExtraBandCode)
  print('\n<search_STAC_Catalog> The total elapsed time for ingesting angles = %6.2f minutes'%(angle_time))

  return stac_items
    


#############################################################################################################
# Description: This function returns a list of unique tile names contained in a given "StatcItems" list.
#
# Revision history:  2024-Jul-17  Lixin Sun  Initial creation
#                    2024-Oct-20  Lixin Sun  Changed "unique_names" from a list to a dictionary, so that the
#                     number of stac items with the same 'grid:code' can be recorded.
#                                            
#############################################################################################################
def get_unique_tile_names(StacItems):
  
  '''
    Args:
      StacItems(List): A list of stac items. 
  '''
  stac_items = list(StacItems)
  unique_names = {}

  if len(stac_items) < 2:
    return unique_names  
  
  unique_names[stac_items[0].properties['grid:code']] = 1

  for item in stac_items:
    new_tile = item.properties['grid:code']
    if new_tile in unique_names:
      unique_names[new_tile] += 1
    else:
      unique_names[new_tile] = 1   
  
  sorted_keys = sorted(unique_names, key = lambda x: unique_names[x], reverse=True)
  
  return sorted_keys 




#############################################################################################################
# Description: This function returns a list of unique STAC items by remaining only one item from those that 
#              share the same timestamp.
#
# Revision history:  2024-Jul-17  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_unique_STAC_items(inSTACItems):
  '''
     Args:
        inSTACItems(): A given list of STAC items to be filtered based on timestamps.
  '''
 
  #==========================================================================================================
  # Retain only one image from the items with identical timestamps
  #==========================================================================================================
  # Create a dictionary to store items by their timestamp
  items_by_id = defaultdict(list)

  # Create a new dictionary with the core image ID as keys
  for item in inSTACItems:    
    tokens = str(item.id).split('_')   #core image ID
    id = tokens[0] + '_' + tokens[1] + '_' + tokens[2]
    items_by_id[id].append(item)
  
  # Iterate through the items and retain only one item per timestamp
  unique_items = []
  for id, item_group in items_by_id.items():
    # Assuming we keep the first item in each group
    unique_items.append(item_group[0])

  return unique_items




#############################################################################################################
# Description: This function returns a list of item names corresponding to a specified MGRS/Snetinel-2 tile.
#
# Note: this function is specifically for Sentinel-2 data, because other dataset might not have 'grid:code'
#       property.
#
# Revision history:  2024-Jul-17  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_one_granule_items(StacItems, GranuleName):
  
  stac_items = list(StacItems)
  tile_items = []

  if len(stac_items) < 2:
    return tile_items  
  
  for item in stac_items:
    if GranuleName == item.properties['grid:code']:
      tile_items.append(item)

  return tile_items 


#############################################################################################################

#############################################################################################################
def ingest_Geo_Angles(StacItems, ExtraBandCode):
  startT = time.time()
  #==========================================================================================================
  # Confirm the given item list is not empty
  #==========================================================================================================
  nItems = len(StacItems)
  if nItems < 1:
    return None
  
  def process_item(item, ExtraBandCode):
    item.properties['sza'] = 90.0 - item.properties['view:sun_elevation']
    item.properties['saa'] = item.properties['view:sun_azimuth']
 
    view_angles = get_View_angles(item)      
    item.properties['vza'] = view_angles['vza']
    item.properties['vaa'] = view_angles['vaa']
    
    return item
  
  #==========================================================================================================
  # Attach imaging geometry angles as properties to each STAC item 
  #==========================================================================================================  
  out_items = []
  # for item in StacItems:
  #   new_item = process_item(item, ExtraBandCode)
  #   out_items.append(new_item)

  with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_item, item, ExtraBandCode) for item in StacItems]
    for future in concurrent.futures.as_completed(futures):
      out_items.append(future.result())
  
  endT   = time.time()
  totalT = (endT - startT)/60

  return out_items, totalT


#############################################################################################################
# Description: This function returns a base image that covers the entire spatial region od an interested area.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-17  Lixin Sun  Modified so that only unique and filtered STAC items will be
#                                            returned 
#                    2024-Dec-02  Marjan Asgari  Modified so that we have a base image backed by a dask array.
#############################################################################################################

def get_base_Image(StacItems, Region, ProjStr, Scale, Criteria, ExtraBandCode):
  
  '''
  Args:
  StacItems(List): A list of STAC items searched for a study area and a time window;
  '''
  #==========================================================================================================
  # Load the first image based on the boundary box of ROI
  #==========================================================================================================
  xy_bbox  = eoUs.get_region_bbox(Region, ProjStr)
  
  base_image_is_read = False
  i = 0
  out_xrDS = None  # To store the dataset once it is loaded

  while not base_image_is_read and i < 20:
      try:
          # If out_xrDS is not None, skip loading data again
          if out_xrDS is not None:
              break  # Data already loaded, break the loop

          # Attempt to load the STAC item and process it
          with odc.stac.load([StacItems[i]],
                            bands  =Criteria['bands'],
                            chunks = {'x': 2000, 'y': 2000},
                            crs = ProjStr, 
                            resolution = Scale, 
                            x = (xy_bbox[0], xy_bbox[2]),
                            y = (xy_bbox[3], xy_bbox[1])) as ds_xr:
              
              # Process the data once loaded successfully
              out_xrDS = ds_xr.isel(time=0).astype(np.float32)
              base_image_is_read = True  # Mark as successfully read
              
      except Exception as e:
          i += 1
          if i >= 20:
              break  # Exit the loop after reaching max retries

  #==========================================================================================================
  # Attach necessary extra bands
  #==========================================================================================================
  band1 = Criteria['bands'][0]  
  out_xrDS[eoIM.pix_date]  = out_xrDS[band1]
  out_xrDS[eoIM.pix_score] = out_xrDS[band1]
  
  if int(ExtraBandCode) == eoIM.EXTRA_ANGLE:
    out_xrDS['cosSZA'] = out_xrDS[band1]
    out_xrDS['cosVZA'] = out_xrDS[band1]
    out_xrDS['cosRAA'] = out_xrDS[band1]
  
  #==========================================================================================================
  # Mask out all the pixels in each variable of "base_img", so they will treated as gap/missing pixels
  # This step is very import if "combine_first" function is used to merge granule mosaic into based image. 
  #==========================================================================================================
  out_xrDS = out_xrDS.where(out_xrDS != out_xrDS, -10000.0)

  return out_xrDS



#############################################################################################################
# Description: This function returns reference bands for the blue and NIR bands.
#
# Revision history:  2024-May-28  Lixin Sun  Initial creation
#                    2024-Nov-26  Marjan Asgari Limiting the calculation of median to only the bands we want
#                                 skiping NA in median calculation.
#############################################################################################################
def get_score_refers(ready_IC):
  
  #==========================================================================================================
  # Extract separate bands from the median image, then calculate NDVI and modeled blue median band
  #==========================================================================================================
  blu = ready_IC['blue'].median(dim='time', skipna=True)
  red = ready_IC['red'].median(dim='time', skipna=True)
  nir = ready_IC['nir08'].median(dim='time', skipna=True)
  sw2 = ready_IC['swir22'].median(dim='time', skipna=True)

  NDVI      = (nir - red)/(nir + red + 0.0001)  
  model_blu = sw2*0.25
  
  #==========================================================================================================
  # Correct the blue band values of median mosaic for the pixels with NDVI values larger than 0.3
  #========================================================================================================== 
  condition = (model_blu > blu) | (NDVI < 0.3) | (sw2 < blu)
  blu       = blu.where(condition, other = model_blu)
  
  del condition, model_blu, NDVI, sw2, red
  return blu, nir


#############################################################################################################
# Description: This function attaches a score band to each image in a xarray Dataset object.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-25  Lixin Sun  Parallelized the code using 'concurrent.futures' module.
#                    2024-Nov-26  Marjan Asgari  Removed the "Parallelized the code using 'concurrent.futures' module."" With dask distributed we cannot use other 
#                                                parallelization techniques.
#############################################################################################################
#==========================================================================================================
# Define an internal function that can calculate time and spectral scores for each image in 'ready_IC'
#==========================================================================================================
def image_score(i, T, ready_IC, midDate, SsrData, median_blu, median_nir):
  
  timestamp  = pd.Timestamp(T).to_pydatetime()
  time_score = get_time_score(timestamp, midDate, SsrData['SSR_CODE'])   
  
  spec_score = get_spec_score(SsrData, ready_IC.isel(time=i), median_blu, median_nir) 
  return i, spec_score * time_score

def attach_score(ready_IC, SsrData, StartStr, EndStr):
  
  '''Attaches a score band to each image in a xarray Dataset object, an image collection equivalent in GEE.
     Args:
       SsrData(dictionary): A dictionary containing some metadata about a sensor;
       ready_ID(xarray.dataset): A xarray dataset object containing a set of STAC images/items;
       StartStr(string): A string representing the start date of a timeframe;
       EndStr(string): A string representing the end date of a timeframe.
  '''

  #==========================================================================================================
  # Create a blue and nir median data arrays
  #==========================================================================================================
  median_blu, median_nir = get_score_refers(ready_IC)
  midDate = datetime.strptime(eoUs.period_centre(StartStr, EndStr), "%Y-%m-%d")
  
  #==========================================================================================================
  # The process of score calculations for every image in 'ready_IC'
  #==========================================================================================================
  time_vals = list(ready_IC.time.values)
  for i, T in enumerate(time_vals):
    i, score = image_score(i, T, ready_IC, midDate, SsrData, median_blu, median_nir)
    ready_IC[eoIM.pix_score][i, :,:] = score
  
  del median_blu, median_nir
  return ready_IC

######################################################################################################
# Description: This function creates a map with all the pixels having an identical time score for a
#              given image. Time score is calculated based on the date gap between the acquisition
#              date of the given image and a reference date (midDate parameter), which normally is
#              the middle date of a time period (e.g., a peak growing season).
#
# Revision history:  2024-May-31  Lixin Sun  Initial creation
#
######################################################################################################
def get_time_score(ImgDate, MidDate, SsrCode):
  '''Return a time score image corresponding to a given image
  
     Args:
        ImgDate (datetime object): A given ee.Image object to be generated a time score image.
        MidData (datetime object): The centre date of a time period for a mosaic generation.
        SsrCode (int): The sensor type code. '''
  
  #==================================================================================================
  # Calculate the date difference between image date and a reference date  
  #==================================================================================================  
  date_diff = (ImgDate - MidDate).days  

  #==================================================================================================
  # Calculatr time score according to sensor type 
  #==================================================================================================
  std = 12 if int(SsrCode) > eoIM.MAX_LS_CODE else 16  

  return 1.0/math.exp(0.5 * pow(date_diff/std, 2))



#############################################################################################################
# Description: This function attaches a score band to each image within a xarray Dataset.
#
# Note:        The given "masked_IC" may be either an image collection with time dimension or a single image
#              without time dimension
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Nov-25  Marjan Asgari   Letting xr.apply_ufunc to use dask arrays
#                                                 Handling dividing by zero in max_SV / max_IR
############################################################################################################# 
def get_spec_score(SsrData, inImg, median_blu, median_nir):
    
    '''Attaches a score band to each image within a xarray Dataset
        Args:
        inImg(xarray dataset): a given single image;
        medianImg(xarray dataset): a given median image.
    '''
  
    blu = inImg[SsrData['BLU']]
    nir = inImg[SsrData['NIR']]
    
    max_SV = xr.apply_ufunc(np.maximum, blu, inImg[SsrData['GRN']], dask='allowed')
    max_SW = xr.apply_ufunc(np.maximum, inImg[SsrData['SW1']], inImg[SsrData['SW2']], dask='allowed')
    max_IR = xr.apply_ufunc(np.maximum, nir, max_SW, dask='allowed')

    #==================================================================================================
    # Calculate scores assuming all the pixels are water
    #==================================================================================================

    water_score = xr.where(max_IR != 0, max_SV / max_IR, np.nan)
    water_score = water_score.where(median_blu > blu, -1*water_score)
    
    #==================================================================================================
    # Calculate scores assuming all the pixels are land
    #==================================================================================================

    blu_pen = blu - median_blu
    nir_pen = median_nir - nir
    STD_blu = blu.where(blu > 0, 0) + 1.0 

    land_score = (max_IR*100.0)/(STD_blu*100.0 + blu_pen + nir_pen)
    land_score = land_score.where((max_SV < max_IR) | (max_SW > 3.0), water_score)
    del STD_blu, nir_pen, blu_pen, water_score, max_IR, max_SW, max_SV, nir,blu
    return land_score



############################################################################################################# 
# This function should be here not in eoImage; Otherwise 1- dask workers get killed sometime 2- The execution time increases
############################################################################################################# 
def attach_AngleBands(xrDS, StacItems):
  
  '''Attaches three angle bands to a satallite SURFACE REFLECTANCE image
  Args:    
    xrDS(xr Dateset): A xarray dataset object (a single image);
    StacItems(List): A list of STAC items corresponding to the "xrDS".
    
  '''  
  #==========================================================================================================
  # Sort the provided STAC items to match the image sequence in "xrDS"
  #==========================================================================================================  
  def get_sort_key(item):
    return item.datetime
  
  sorted_items = sorted(StacItems, key=get_sort_key)
  
  #==========================================================================================================
  # Create three lists to store the cosine values of the imaging geometry angles. 
  #==========================================================================================================  
  cosSZAs = np.cos(np.radians([item.properties['sza'] for item in sorted_items]))
  cosVZAs = np.cos(np.radians([item.properties['vza'] for item in sorted_items]))
  cosRAAs = np.cos(np.radians([item.properties['saa'] - item.properties['vaa'] for item in sorted_items]))

  #==========================================================================================================
  # Define a function to map indices to the angle cosine values
  #==========================================================================================================
  def map_indices_to_values(IndxBand, values):
    indx_band = IndxBand.astype(np.int8)
    return values[indx_band]
  
  #==========================================================================================================
  # Apply the function using xarray.apply_ufunc
  #==========================================================================================================
  xrDS["cosSZA"] = xr.apply_ufunc(map_indices_to_values, xrDS["time_index"], cosSZAs).astype(np.float32)
  xrDS["cosVZA"] = xr.apply_ufunc(map_indices_to_values, xrDS["time_index"], cosVZAs).astype(np.float32)
  xrDS["cosRAA"] = xr.apply_ufunc(map_indices_to_values, xrDS["time_index"], cosRAAs).astype(np.float32)
  del cosRAAs, cosVZAs, cosSZAs
  
  return xrDS

#############################################################################################################
# Description: 
# 
# Revision history:  2024-Nov-25  Marjan Asgari   This function is not a dask delayed anymore, because we are using dask arrays inside it
#                                                 Not loading one_DS in memory and instead we keep it as a dask array 
#                                                 max indices now should be computed before using it for slicing the mosaic array  
#
############################################################################################################# 
def get_granule_mosaic(args):

  '''
    Args:
      SsrData(Dictionary): Some meta data on a used satellite sensor;
      TileItems(List): A list of STAC items associated with a specific tile;
      ExtraBandCode(Int): An integer indicating if to attach extra bands to mosaic image.
  '''
  base_img, granule_name, stac_items, SsrData, StartStr, EndStr, Bands, ProjStr, Scale, ExtraBandCode = args
  try:
    
    os.environ['http_proxy'] = "http://webproxy.science.gc.ca:8888/"
    os.environ['https_proxy'] = "http://webproxy.science.gc.ca:8888/"
    
    one_granule_items = get_one_granule_items(stac_items, granule_name)
    filtered_items    = get_unique_STAC_items(one_granule_items)
    
    successful_items = []
    attempt = 0
    while attempt < 15:
      try:
        xrDS = odc.stac.load(filtered_items,  # List of STAC items
                            bands  = Bands,
                            chunks = {'x': 2000, 'y': 2000},
                            crs    = ProjStr, 
                            fail_on_error = False,
                            resolution    =  Scale,
                            preserve_original_order = True)
        break
      except (TimeoutError, ConnectionError) as e:
        print(f"Proxy connection error: {e}. Retrying {attempt + 1}/5...")
        xrDS = None
        attempt += 1
        if attempt < 15:
          time.sleep(10)
      except Exception as e:
        print(f"An error occurred: {e}")
        xrDS = None 
        break

    if xrDS is None:
      return None
    
    #==========================================================================================================
    # Attach three layers, an empty 'score', acquisition DOY and 'time_index', to each item/image in "xrDS" 
    #==========================================================================================================  
    time_values = xrDS.coords['time'].values 
    time_datetime = pd.to_datetime(time_values)
    doys = [date.timetuple().tm_yday for date in time_datetime]  #Calculate DOYs for every temporal point
    
    xrDS[eoIM.pix_score] = xrDS[SsrData['BLU']]*0
    xrDS[eoIM.pix_date]  = xr.DataArray(da.from_array(np.array(doys, dtype='uint16'), chunks=1) , dims=['time'])
    #==========================================================================================================
    # Apply default pixel mask, rescaling gain and offset to each image in 'xrDS'
    #==========================================================================================================
    xrDS = eoIM.apply_default_mask(xrDS, SsrData)
    xrDS = eoIM.apply_gain_offset(xrDS, SsrData, 100, False)
    
    #==========================================================================================================
    # Calculate compositing scores for every valid pixel in xarray dataset object (xrDS)
    #==========================================================================================================
    attach_score_args = functools.partial(attach_score, 
      SsrData=SsrData, 
      StartStr=StartStr,
      EndStr=EndStr
    )
    xrDS = xrDS.chunk({'x': 2000, 'y': 2000, 'time': xrDS.sizes['time']}).map_blocks(
      attach_score_args, 
      template=xrDS.chunk({'x': 2000, 'y': 2000, 'time': xrDS.sizes['time']})
    )
    #==========================================================================================================
    # Create a composite image based on compositing scores
    # Note: calling "fillna" function before invaking "argmax" function is very important!!!
    #==========================================================================================================
    xrDS = xrDS.fillna(-0.0001).chunk({'x': 2000, 'y': 2000, 'time': xrDS.sizes['time']})
    
    def granule_mosaic_template(xrDS, extra_code, extra_angle):
      
      mosaic_template = {}
      
      xrDA = xr.DataArray(
        data=da.zeros(
          (xrDS.sizes['y'], xrDS.sizes['x']),  # Shape includes only y, x (time is handled separately)
          chunks=(2000, 2000), 
          dtype=np.float32
        ),
        dims=['y', 'x'],  # Include y and x only (no time here)
        coords={'y': xrDS['y'], 'x': xrDS['x']},
      )
      for var_name in xrDS.data_vars:
        mosaic_template[var_name] = xrDA
      if extra_code == extra_angle:
        mosaic_template["cosSZA"]=  xrDA
        mosaic_template["cosVZA"] = xrDA
        mosaic_template["cosRAA"] = xrDA
      
      return xr.Dataset(mosaic_template)
    
    def granule_mosaic(xrDS, extra_code, filtered_items, pix_score, extra_angle, time_values_len):
      

      xrDS['time_index']   = xr.DataArray(np.array(range(0, time_values_len), dtype='uint8'), dims=['time'])
      max_indices = xrDS[pix_score].argmax(dim="time")
      mosaic = xrDS.isel(time=max_indices)
      
      #==========================================================================================================
      # Attach an additional bands as necessary 
      #==========================================================================================================
      if extra_code == extra_angle:
        mosaic = attach_AngleBands(mosaic, filtered_items)
      
      #==========================================================================================================
      # Remove 'time_index', 'time', and 'spatial_ref' variables from submosaic 
      #==========================================================================================================
      return mosaic.drop_vars(["time", "spatial_ref", "time_index"])
    
    granule_mosaic_mosaic_args = functools.partial(granule_mosaic, 
      extra_code      = int(ExtraBandCode), 
      filtered_items  = filtered_items,
      pix_score       = eoIM.pix_score,
      extra_angle     = eoIM.EXTRA_ANGLE,
      time_values_len = len(time_values),
    )
    mosaic = xrDS.map_blocks(
        granule_mosaic_mosaic_args, 
        template=granule_mosaic_template(xrDS, int(ExtraBandCode), eoIM.EXTRA_ANGLE)  # Pass the template (same structure as xrDS)
    )
    
    mosaic = mosaic.where(mosaic[eoIM.pix_date] > 0)
    mosaic = mosaic.reindex_like(base_img).chunk({'x': 2000, 'y': 2000})
    
    del xrDS, granule_mosaic_mosaic_args
    gc.collect()
    
    return mosaic
  except Exception as e:
    print(f"Exception occurred: {e}")
    raise  # Reraise the exception for proper handling in the pool


#############################################################################################################
# Description: This function create a composite image by gradually merge the granule mosaics from 
#               "get_granule_mosaic" function.
# 
# Revision history:  2024-Oct-18  Lixin Sun  Initial creation
#
#############################################################################################################
def create_mosaic_at_once_distributed(base_img, unique_granules, stac_items, SsrData, StartStr, EndStr, criteria, ProjStr, Scale, ExtraBandCode, base_directory, n_nodes, n_workers_per_node, memory_limit):
  
  Bands = criteria['bands']
  unique_name = str(uuid.uuid4())
  
  tmp_directory = os.path.join(Path(base_directory), f"dask_spill_{unique_name}")
  if not os.path.exists(tmp_directory):
    os.makedirs(tmp_directory)

  logging.getLogger('tornado').setLevel(logging.WARNING)
  logging.getLogger('tornado.application').setLevel(logging.CRITICAL)
  logging.getLogger('tornado.access').setLevel(logging.CRITICAL)
  logging.getLogger('tornado.general').setLevel(logging.CRITICAL)
  logging.getLogger('bokeh.server.protocol_handler').setLevel(logging.CRITICAL)

  count = 0 
  for granule in unique_granules:
    one_granule_items = get_one_granule_items(stac_items, granule)
    filtered_items    = get_unique_STAC_items(one_granule_items)
    count = count + len(filtered_items)

  print(f'\n\n<<<<<<<<<< The count of all unique stack items is {count} >>>>>>>>>')
  
  with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    # these should be imported here due to "ValueError: signal only works in main thread of the main interpreter"
    from dask.distributed import Client
    from dask_jobqueue import SLURMCluster
    from dask import config
    
    def disable_spill():
      dask.config.set({
        'distributed.comm.retry.count': 5,
        'distributed.comm.timeouts.tcp' : 18000,
        'distributed.comm.timeouts.connect': 18000,
        'distributed.worker.memory.target': 1, 
        'distributed.worker.memory.spill': 0.95,
        'distributed.worker.memory.terminate': 0.95 
        })
    
    if n_workers_per_node != 1 and n_nodes != 1:
      num_nodes  = n_nodes
      memory     = memory_limit
      processes  = n_workers_per_node
      cores      = processes * 4
    else: 
      num_nodes = 5
      processes = 4
      cores     = processes * 5
      if count >= 1000 and Scale <= 30:
        num_nodes = min(int(len(unique_granules) / processes), 10)
      memory    = "480G"
    
    os.environ['http_proxy'] = "http://webproxy.science.gc.ca:8888/"
    os.environ['https_proxy'] = "http://webproxy.science.gc.ca:8888/"
    out_file = Path(base_directory) / f"log_{unique_name}.out"
    
    cluster = SLURMCluster(
      account='nrcan_geobase',     # SLURM account
      queue='standard',        # SLURM partition (queue)
      walltime='06:00:00',     
      cores=cores,
      processes=processes,      
      memory=memory,
      local_directory=tmp_directory,
      worker_extra_args =[f"--memory-limit='{memory}'"],
      job_script_prologue = ["export http_proxy=http://webproxy.science.gc.ca:8888/", "export https_proxy=http://webproxy.science.gc.ca:8888/"],
      job_extra_directives = [f" --output={out_file}"]
    )
    cluster.scale_up(n=num_nodes, memory=memory, cores=cores)
    client = Client(cluster, timeout=3000)
    client.register_worker_callbacks(setup=disable_spill)
    
    print(f'\n\n<<<<<<<<<< Dask dashboard is available {client.dashboard_link} >>>>>>>>>')
    
    while True:
      workers_info = client.scheduler_info()['workers']
      if len(workers_info) >= num_nodes * processes:
        print(f"Cluster has {len(workers_info)} workers. Proceeding...")
        break 
      else:
        print(f"Waiting for workers. Currently have {len(workers_info)} workers.")
        time.sleep(5)
    worker_names = [info['name'] for worker, info in workers_info.items()]
    
    # we submit the jobs to the cluster to process them in a distributed manner 
    granule_mosaics_data = [(base_img, granule, stac_items, SsrData, StartStr, EndStr, Bands, ProjStr, Scale, ExtraBandCode) for granule in unique_granules]
    granule_mosaics = []

    for i in range(len(granule_mosaics_data)):
      worker_index = i % len(worker_names) 
      granule_mosaics.append(client.submit(get_granule_mosaic, granule_mosaics_data[i], workers=worker_names[worker_index], allow_other_workers=True))

    return granule_mosaics, client, cluster

def create_mosaic_at_once(base_img, unique_granules, stac_items, SsrData, StartStr, EndStr, criteria, ProjStr, Scale, ExtraBandCode, base_directory, n_workers, memory_limit):
  
  Bands = criteria['bands']
  
  tmp_directory = os.path.join(Path(base_directory), "tmp_directory")
  if not os.path.exists(tmp_directory):
    os.makedirs(tmp_directory)
  

  logging.getLogger('tornado').setLevel(logging.WARNING)
  logging.getLogger('tornado.application').setLevel(logging.CRITICAL)
  logging.getLogger('tornado.access').setLevel(logging.CRITICAL)
  logging.getLogger('tornado.general').setLevel(logging.CRITICAL)
  logging.getLogger('bokeh.server.protocol_handler').setLevel(logging.CRITICAL)

  with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    # these should be imported here due to "ValueError: signal only works in main thread of the main interpreter"
    from dask.distributed import Client
    from dask.distributed import LocalCluster
    from dask import config

    dask.config.set({
      'distributed.comm.retry.count': 5,
      'distributed.comm.timeouts.tcp' : 180,
      'distributed.comm.timeouts.connect': 180,
      'distributed.worker.memory.target': 1, 
      'distributed.worker.memory.spill': 0.95,
      'distributed.worker.memory.terminate': 0.95 
    })
    
    slurm_job_id = os.getenv('SLURM_JOB_ID')
    if slurm_job_id:
      threads = get_slurm_node_cpu_cores()
    else:
      import psutil
      threads = psutil.cpu_count(logical=False)
    
    cluster = LocalCluster(
      n_workers = n_workers,
      threads_per_worker = int(threads/n_workers) if threads != 1 else 1,
      memory_limit = f"{memory_limit}G",
      local_directory = tmp_directory,
    )
    client = Client(cluster)
    print(f'\n\n<<<<<<<<<< Dask dashboard is available {client.dashboard_link} >>>>>>>>>')
  
    # we submit the jobs to the cluster to process them in a distributed manner 
    granule_mosaics_data = [(base_img, granule, stac_items, SsrData, StartStr, EndStr, Bands, ProjStr, Scale, ExtraBandCode) for granule in unique_granules]
    granule_mosaics = [client.submit(get_granule_mosaic, data) for data in granule_mosaics_data]

    return granule_mosaics, client

#############################################################################################################
# Description: This function returns a composite image generated from images acquired over a specified time 
#              period.
# 
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-20  Lixin Sun  Modified to generate the final composite image tile by tile.
#############################################################################################################
def period_mosaic(inParams, ExtraBandCode, number_nodes, number_workers , memory_full_node, debug_mode):
  
  '''
    Args:
      inParams(dictionary): A dictionary containing all necessary execution parameters;
      ExtraBandCode(int): An integer indicating which kind of extra bands will be created as well.
  '''
  
  mosaic_start = time.time()  

  #==========================================================================================================
  # Confirm 'current_month' and 'current_tile' keys have valid values
  #==========================================================================================================
  StartStr, EndStr = eoPM.get_time_window(inParams)
  if StartStr == None or EndStr == None:
    print('\n<period_mosaic> Invalid time window was defined!!!')
    return None
  
  Region = eoPM.get_spatial_region(inParams)
  if Region == None:
    print('\n<period_mosaic> Invalid spatial region was defined!!!')
    return None
  
  #==========================================================================================================
  # Prepare other required parameters and query criteria
  #==========================================================================================================
  ProjStr = str(inParams['projection']) if 'projection' in inParams else 'EPSG:3979'
  Scale   = int(inParams['resolution']) if 'resolution' in inParams else 20

  SsrData  = eoIM.SSR_META_DICT[str(inParams['sensor']).upper()]
  cloud_cover = eoPM.get_cloud_coverage(inParams)
  criteria = get_query_conditions(SsrData, StartStr, EndStr, cloud_cover)
  
  #==========================================================================================================
  # Search all the STAC items based on a spatial region and a time window
  # Note: (1) The third parameter (MaxImgs) for "search_STAC_Catalog" function cannot be too large. Otherwise,
  #           a server internal error will be triggered.
  #       (2) The imaging angles have been attached to each STAC item by "search_STAC_Catalog" function.
  #==========================================================================================================  
  stac_items = search_STAC_Catalog(Region, criteria, 100, ExtraBandCode)

  print(f"\n<period_mosaic> A total of {len(stac_items):d} items were found.\n")
  
  #==========================================================================================================
  # Create a base image that has full spatial dimensions covering ROI
  #==========================================================================================================
  base_img = get_base_Image(stac_items, Region, ProjStr, Scale, criteria, ExtraBandCode)
  
  print('\n<period_mosaic> based mosaic image = ', base_img)
  #==========================================================================================================
  # Get a list of unique tile names and then loop through each unique tile to generate submosaic 
  #==========================================================================================================  
  unique_granules = get_unique_tile_names(stac_items)  #Get all unique tile names 
  print('\n<period_mosaic> The number of unique granule tiles = %d'%(len(unique_granules)))  
  print('\n<<<<<< The unique granule tiles = ', unique_granules) 
  

  #==========================================================================================================
  # Run the get unique_granules in parallel and on distributed workers
  #==========================================================================================================
  if debug_mode:
    submited_granules_mosaics, client = create_mosaic_at_once(base_img, unique_granules, stac_items, SsrData, StartStr, EndStr, criteria, ProjStr, Scale, ExtraBandCode, inParams['out_folder'], number_workers , memory_full_node)
  else:
    submited_granules_mosaics, client, cluster = create_mosaic_at_once_distributed(base_img, unique_granules, stac_items, SsrData, StartStr, EndStr, criteria, ProjStr, Scale, ExtraBandCode, inParams['out_folder'], number_nodes, number_workers, memory_full_node)
  
  persisted_granules_mosaics = dask.persist(*submited_granules_mosaics, optimize_graph=True)
  for future, granules_mosaic in as_completed(persisted_granules_mosaics, with_results=True):
    base_img = merge_granule_mosaics(granules_mosaic, base_img, eoIM.pix_score)
    client.cancel(future)
  
  # We do the compute to get a dask array instead of a future
  base_img = base_img.chunk({"x": 2000, "y": 2000}).compute()

  #==========================================================================================================
  # Mask out the pixels with negative date value
  #========================================================================================================== 
  mosaic = base_img.where(base_img[eoIM.pix_date] > 0)

  mosaic_stop = time.time()
  mosaic_time = (mosaic_stop - mosaic_start)/60
  try:
    client.close()
    cluster.close()
  except asyncio.CancelledError:
    print("Cluster is closed!")

  print('\n\n<<<<<<<<<< The total elapsed time for generating the mosaic = %6.2f minutes>>>>>>>>>'%(mosaic_time))
  return mosaic

@delayed
def merge_granule_mosaics(mosaic, base_img, pix_score):
  
  """
  Process a single mosaic by applying the mask and updating base_img.
  This function will be executed on Dask workers.
  """
  if mosaic is not None:
    mask   = mosaic[pix_score] > base_img[pix_score]
    for var in base_img.data_vars:
      base_img[var] = base_img[var].where(~mask, mosaic[var], True)
    return base_img  # Return the updated base_img
  return None  # If mosaic is None, return None

#############################################################################################################
# Description: This function exports the band images of a mosaic into separate GeoTiff files
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Dec-04  Marjan Asgari Add .compute() on the mosaic before saving it to tiff files
#############################################################################################################
def export_mosaic(inParams, inMosaic):
  '''
    This function exports the band images of a mosaic into separate GeoTiff files.

    Args:
      inParams(dictionary): A dictionary containing all required execution parameters;
      inMosaic(xrDS): A xarray dataset object containing mosaic images to be exported.'''
  
  #==========================================================================================================
  # Get all the parameters for exporting composite images
  #==========================================================================================================
  params = eoPM.get_mosaic_params(inParams)  

  #==========================================================================================================
  # Convert float pixel values to integers
  #==========================================================================================================
  mosaic_int = (inMosaic * 100.0).astype(np.int16)
  rio_mosaic = mosaic_int.rio.write_crs(params['projection'], inplace=True)  # Assuming WGS84 for this example

  #==========================================================================================================
  # Create a directory to store the output files
  #==========================================================================================================
  dir_path = params['out_folder']
  os.makedirs(dir_path, exist_ok=True)

  #==========================================================================================================
  # Create prefix filename
  #==========================================================================================================
  SsrData    = eoIM.SSR_META_DICT[str(params['sensor'])]   
  
  region_str = str(params['current_region'])
  period_str = str(params['time_str'])
 
  filePrefix = f"{SsrData['NAME']}_{region_str}_{period_str}"

  #==========================================================================================================
  # Create individual sub-mosaic and combine it into base image based on score
  #==========================================================================================================
  spa_scale    = params['resolution']
  export_style = str(params['export_style']).lower()
  rio_mosaic = rio_mosaic.compute()
  ext_saved =[]
  if 'sepa' in export_style:
    for band in rio_mosaic.data_vars:
      out_img     = rio_mosaic[band]
      filename    = f"{filePrefix}_{band}_{spa_scale}m.tif"
      output_path = os.path.join(dir_path, filename)
      out_img.rio.to_raster(output_path)
      ext_saved.append(band)
  else:
    filename = f"{filePrefix}_mosaic_{spa_scale}m.tif"
    output_path = os.path.join(dir_path, filename)
    rio_mosaic.to_netcdf(output_path)
    ext_saved.append("mosaic")
  
  return ext_saved, period_str


def get_slurm_node_cpu_cores():
  import subprocess
  result = subprocess.check_output(f"scontrol show job {os.getenv('SLURM_JOB_ID')}", shell=True).decode()
  for line in result.splitlines():
      if 'TresPerTask' in line:
          tres_per_task = line.split("=")[1]
          if 'cpu:' in tres_per_task:
              cpu_count = tres_per_task.split('cpu:')[1]
              try:
                  cpu_count = int(cpu_count)
                  return cpu_count
              except ValueError:
                  return 1
          else:
              return 1