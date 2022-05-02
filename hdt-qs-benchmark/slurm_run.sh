#!/bin/bash

#SBATCH -J "QA-BSBM"
#SBATCH -n 1 # Tasks
#SBATCH -c 1 # CPUs
#SBATCH --mem=40G
#SBATCH -p "GPU-DEPINFO" # partition
#SBATCH -t 07:00:00:00
#SBATCH -o output_%j.out
#SBATCH -e output_%j.err

srun ./benchmark.sh
