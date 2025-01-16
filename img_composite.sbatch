#! /bin/bash -l

#SBATCH --job-name=imagery-composite
#SBATCH --cluster=gpsc7 
#SBATCH --output=/gpfs/fs5/nrcan/nrcan_geobase/work/dev/datacube/parallel/tile42_seasonal/imagery-composite.out
#SBATCH --account=nrcan_geobase
#SBATCH --partition=standard
#SBATCH --time=00-10:00:00
#SBATCH --qos=low
#SBATCH --mem=200G
#SBATCH --comment="image=registry.maze.science.gc.ca/ssc-hpcs/generic-job:ubuntu22.04"

# activating the conda env
source /space/partner/nrcan/geobase/work/opt/miniconda-datacube/etc/profile.d/conda.sh
conda activate img_composite_env

# set up the proxies
export http_proxy=http://webproxy.science.gc.ca:8888/
export https_proxy=http://webproxy.science.gc.ca:8888/

# set up the directory and run the python command

cd /gpfs/fs5/nrcan/nrcan_geobase/work/dev/datacube/parallel/STAC_python/STAC_python-main
python -u main.py \
  -y 2023 \
  -m 5 6 7 8 9 10 \
  -t tile42 \
  -ct 0 \
  -sd 2023-05-01 \
  -ed 2023-10-30 \
  -o /gpfs/fs5/nrcan/nrcan_geobase/work/dev/datacube/parallel/tile55_seasonal/ \
  -et
 
 
 