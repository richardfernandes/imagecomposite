import os
import sys
import copy
import argparse
from pathlib import Path
#Get the absolute path to the parent of current working directory 
cwd    = os.getcwd()
source_path = os.path.join(cwd, 'source')
sys.path.append(source_path)
if str(Path(__file__).parents[0]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parents[0]))

import source.eoMosaic as eoMz
import source.eoImage as eoIM
import source.eoParams as eoPM



def gdal_mosaic_rasters(sorted_files_to_mosaic:list, merge_output_file:str):
        
    
    options = ['-of', 'GTiff', '-v']
    gdal_merge_command = ['gdal_merge.py', '-o', merge_output_file] + options + sorted_files_to_mosaic
    os.system(" ".join(gdal_merge_command))


def cmd_arguments(argv=None):
    
    """
    Parse command line arguments.
    """
    
    parser = argparse.ArgumentParser(
        usage="%(prog)s [-h HELP] use -h to get supported arguments.",
        description="Mosaic High-resolution imagery using STAC and Xarray",
    )

    parser.add_argument(
        '-s', '--sensor', 
        type=str, 
        default='S2_SR', 
        choices=['S2_SR', 'L8_SR', 'MOD_SR'], 
        help="Sensor type (e.g., 'S2_SR', 'L8_SR', 'MOD_SR')"
    )
    parser.add_argument(
        '-cl', '--cloud_cover', 
        type=int, 
        default=85.0, 
        help="Cloud cover "
    )
    parser.add_argument(
        '-u', '--unit', 
        type=int, 
        default=2,  
        choices=[1, 2], 
        help="Data unit code (1 for TOA, 2 for surface reflectance)"
    )
    parser.add_argument(
        '-y', '--year', 
        type=int, 
        help="Image acquisition year"
    )
    parser.add_argument(
        '-nby', '--nbyears', 
        type=int, 
        default=-1, 
        help="Positive integer for annual product, or negative for monthly product"
    )
    parser.add_argument(
        '-m', '--months', 
        type=int, 
        nargs='+', 
        choices=range(1, 13), 
        help="List of months included in the product (e.g., 5 6 7 8 9 10)"
    )
    parser.add_argument(
        '-sd', '--start_dates', 
        type=str, 
        default="", 
        nargs='+', 
        help="List of start dates (e.g., '2023-05-01')"
    )
    parser.add_argument(
        '-ed', '--end_dates', 
        type=str, 
        default="", 
        nargs='+', 
        help="List of end dates (e.g., '2023-10-30')"
    )
    parser.add_argument(
        '-t', '--tile_names', 
        type=str, 
        nargs='+', 
        help="List of (sub-)tile names"
    )
    parser.add_argument(
        '-pn', '--prod_names', 
        type=str, 
        nargs='+', 
        default=['LAI', 'fCOVER', 'fAPAR', 'Albedo'], 
        help="List of product names (e.g., 'mosaic', 'LAI', 'fCOVER')"
    )
    parser.add_argument(
        '-r', '--resolution', 
        type=int, 
        default=20, 
        help="Spatial resolution (default: 20)"
    )
    parser.add_argument(
        '-o', '--out_folder', 
        type=str, 
        help="Folder name for exporting results"
    )
    parser.add_argument(
        '-proj', '--projection', 
        type=str, 
        default='EPSG:3979', 
        help="Projection (e.g., 'EPSG:3979')"
    )
    parser.add_argument(
        '-d', '--debug', 
        action='store_true', 
        help="Run the program in debug mode. Creates a single-node Dask cluster."
    )
    parser.add_argument(
        '-et', '--entire_tile', 
        action='store_true', 
        help="Mosaic the entire tile. Program will run for all 9 subtile sections."
    )
    parser.add_argument(
        '-nw', '--number_workers', 
        type=int, 
        default=1, 
        help="Number of Dask workers. Set based on available cores and nodes."
    )
    parser.add_argument(
        '-nm', '--node_memory', 
        type=str, 
        default=-1, 
        help="Memory allocated for each Dask worker."
    )
    parser.add_argument(
        '-n', '--nodes', 
        type=int, 
        default=1, 
        help="Number of physical nodes for distributed Dask mode"
    )


    args = parser.parse_args()
    
    sensor     = args.sensor 
    nbyears    = args.nbyears
    cloud_cover= args.cloud_cover
    year       = args.year
    unit       = args.unit
    months     = args.months
    tile_names = args.tile_names
    prod_names = args.prod_names 
    resolution = args.resolution 
    out_folder = args.out_folder
    projection = args.projection
    start_dates  = args.start_dates
    end_dates    = args.end_dates
    debug        = args.debug
    entire_tile  = args.entire_tile
    number_workers = args.number_workers
    nodes          = args.nodes if not debug else 1
    node_memory    = args.node_memory

    params = {
        "sensor" : sensor,
        "unit" : unit, 
        "year" : year,
        "cloud_cover" : cloud_cover,
        "nbYears" : nbyears,
        "months" : months,
        "tile_names" : tile_names,
        "prod_names" : prod_names,
        "resolution" : resolution,
        "out_folder" : out_folder,
        "projection" : projection,
    }
    if 'end_dates' != "":
        params["end_dates"] = end_dates
    if 'start_dates' != "": 
        params["start_dates"] = start_dates
    
    compute_arguments = {
        "debug"       : debug,
        "entire_tile" : entire_tile,
        "nodes"       : nodes,
        "node_memory" : node_memory,
        "number_workers" : number_workers
    }
    
    return params, compute_arguments


def main():
    
    params , compute_arguments = cmd_arguments()

    ext_tiffs_rec = []
    period_str = ""
    all_base_tiles = []
    
    for base_tile_name in params['tile_names']:
        
        if compute_arguments["entire_tile"]:
            subtiles = [ "912", "913"]
            for subtile in subtiles:
                
                tile_params = copy.deepcopy(params)
                tile_params['tile_names'] = [base_tile_name + "_"+ f'{subtile}']
                tile_params = eoPM.get_mosaic_params(tile_params)

                mosaic = eoMz.period_mosaic(tile_params, eoIM.EXTRA_ANGLE, number_nodes = compute_arguments["nodes"], number_workers = compute_arguments["number_workers"],
                                                memory_full_node = compute_arguments["node_memory"], debug_mode=compute_arguments["debug"])
                if len(ext_tiffs_rec) == 0:
                    ext_tiffs_rec, period_str = eoMz.export_mosaic(tile_params, mosaic)
                else:
                    eoMz.export_mosaic(tile_params, mosaic)

                all_base_tiles.append(base_tile_name)
        else:
            tile_params = eoPM.get_mosaic_params(tile_params)
            mosaic = eoMz.period_mosaic(tile_params, eoIM.EXTRA_ANGLE, number_nodes = compute_arguments["nodes"], number_workers = compute_arguments["number_workers"],
                                            memory_full_node = compute_arguments["node_memory"], debug_mode=compute_arguments["debug"])
            eoMz.export_mosaic(tile_params, mosaic)
    
    
    if compute_arguments["entire_tile"]:
        subtiles = ["911", "912", "913", "921", "922", "923", "931", "932", "933"]
        for base_tile in all_base_tiles:
            for product in ext_tiffs_rec:
                gdal_files = []
                for subtile in subtiles:
                    file_path = params["out_folder"] + "/" + params["sensor"] + "_" + base_tile + "_" + f'{subtile}' + "_" + period_str + "_" + product + "_" + str(params["resolution"]) + "m.tif"
                    if os.path.exists(Path(file_path)):
                        gdal_files.append(file_path)
                    else:
                        break
                if len(gdal_files) == len(subtiles):
                    merge_output = params["out_folder"] + "/" + params["sensor"] + "_" + base_tile + "_" + period_str + "_" + product + "_" + str(params["resolution"]) + "m.tif"
                    gdal_mosaic_rasters(gdal_files,merge_output)
    

if __name__ == "__main__":
    main()
