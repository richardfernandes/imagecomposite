# STAC_python



There are 2 options for running STACk python, which are 1-debug and 2-distributed modes.

The debug mode is coded to be run on a single (personal or HPC) computer. By seting `-d` flag, the program will be run in the debug mode.

The distributed mode is coded to run on a cluster of dask worker on HPC. The number of workers, memory and number of nodes can be set; However, they are coded in the program and we suggest to keep them as default and change them while there is a good knowledge of the data and workers.

*Note #1*
For running the program on HPC, we need to set two following env variables before running the program. The reason for that is the fact that the program constantly has HTTP requests and to fulfill those requests, the node should have access to the proxies.


```bash
export http_proxy=http://webproxy.science.gc.ca:8888/
export https_proxy=http://webproxy.science.gc.ca:8888/
```
*Note #2*

Before running the project, the path should be changed to where the source folder of the project resides. Thus, it require the project to be on HPC if running on HPC:

```bash
cd /../../../nrcan_geobase/work/dev/.../../STAC_python/imagery-composite
```


*Here's an example of how to use STAC_python (Command line and Script):*

**Command line**
```bash
python main -y <year> -m <months> -t <tile_names> -o <out_folder> -s <sensor> -u <unit> -nby <nbyears> -pn <prod_names> -r <resolution> -proj <projection> -cl <cloud_cover> -sd <start_dates> -ed <end_dates> -nw <number_workers> -nm <node_memory> -n <nodes> -d  -et
```
- `-y`, `--year`: Image acquisition year
- `-m`, `--months`: List of months included in the product (e.g., 5 6 7 8 9 10)
- `-t`, `--tile_names`: List of (sub-)tile names
- `-o`, `--out_folder`: Folder name for exporting
- `-s`, `--sensor`: Sensor type (e.g., 'S2_SR', 'L8_SR', 'MOD_SR') (Optional: default is S2_SR)
- `-u`, `--unit`: Data unit code (1 for TOA, 2 for surface reflectance) (Optional: default is 1)
- `-nby`, `--nbyears`: Positive integer for annual product, or negative for monthly product (Optional: default is -1)
- `-pn`, `--prod_names`: List of product names (e.g., 'mosaic', 'LAI', 'fCOVER') (Optional: default is ['LAI', 'fCOVER', 'fAPAR', 'Albedo'])
- `-r`, `--resolution`: Spatial resolution (Optional: default is 20)
- `-proj`, `--projection`: Projection (e.g., 'EPSG:3979') (Optional: default is EPSG:3979)
- `-cl`, `--cloud_cover`:cloud_cover (Optional: default is 85.5)
- `-sd`, `--start_dates`: List of start dates (e.g., '2023-05-01') (Optional: default is NA)
- `-ed`, `--end_dates`: List of end dates (e.g., '2023-05-01') (Optional: default is NA)
- `-nw`, `--number_workers`: The number of total dask workers to run the program. The number should be set based on the number of cores and physical nodes available for dask. (Optional: default is set based on the avaialbel stack items and debug mode)
- `-nm`, `--node_memory`: The amount of memory for each dask worker. The memory should be set based on the available memory on each node and number of dask workers running on each physical node (Optional: default is set based on the avaialbel stack items and debug mode)
- `-n`, `--nodes`:The number of physical nodes in distributed dask mode (Optional: default is set based on the avaialbel stack items and debug mode)
- `-d`, `--debug`: Run the program in debug mode. In the debug mode, dask will be creating its cluster on a single physical node.
- `-et`, `--entire_tile`: Mosaic the entire tile. By setting this argument, the program will be run for all 9 subtitles of the requested tile.


NOTE: You are required to either provide months or a customized start and end date for the temporal aspect. If you provide one or a list of months, the start date will be the first day of the first month, and the end date will be the last day of the last month.
For an example:

```bash
python main.py -y 2023 -t tile55  0 -sd 2023-05-01 -ed 2023-10-30 -o .../tile55_seasonal/ -et

```

```bash
python main.py -y 2023 -m 5 6 7 8 9 10 -t tile55  -o .../tile55_seasonal/ -et

```
