#!/bin/bash
#SBATCH --partition=health,lts,hawkcpu,infolab,engi,eng

# Request 1 hour of computing time
#SBATCH --time=1:00:00
#SBATCH --ntasks=1

# Give a name to your job to aid in monitoring
#SBATCH --job-name covidForecastingvaccinedata

# Write Standard Output and Error
#SBATCH --output="myjobvaccinedata.%j.%N.out"

cd ${SLURM_SUBMIT_DIR} # cd to directory where you submitted the job

export PYTHONPATH=$PYTHONPATH:$HOME/pythonpkgs
python3 download_vaccine_data.py

exit
