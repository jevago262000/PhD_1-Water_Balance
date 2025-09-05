# This script averages/sums the monthly water balance variables into multiannual monthly variables
# WARNING: This script crashes when the input path ("wd") has spaces

# Author: Jefferson Valencia Gomez
# Email: j.valencia@cgiar.org, jefferson.valencia.gomez@gmail.com
# Year: 2017

import os
import fnmatch
import glob
import arcpy
from arcpy import env
from arcpy.sa import *

# Environment variables
arcpy.CheckOutExtension("spatial")
env.overwriteOutput = True
env.cellSize = "MINOF"

# Clean terminal
os.system('cls')

# Input parameters
#wd = input("Enter the path of the variable to be precessed: ")
rangeYears = input("Type the interval of years to process (e.g., '1958-2023') "
                    "or just press Enter to take the interval by default (including a 6-yr warm-up period): ")
variable = int(input("Type 1: 'ppt', 2: 'pet', 3: 'q', 4: 'eprec', 5: 'aet', 6: 'perc', 7: 'sstor', 8: 'bflow', 9: 'bflow3', 10: 'wyield', 11: 'wyield3': "))

# Options of variables
weather_vars = {1: 'ppt', 2: 'pet', 3: 'q', 4: 'eprec', 5: 'aet', 6: 'perc', 7: 'sstor', 8: 'bflow', 9: 'bflow3', 10: 'wyield', 11: 'wyield3'}
bands_gee = ["pr", "pet", "ro"] # band names in gee
tc_vars = ["ppt", "pet", "q"] # variable names according to TerraClimate

# Empty wildcard for GRID rasters
#wildcard = ""
wildcard = ".tif"

# Variable to be processed
working_var = weather_vars[variable]

if working_var in tc_vars:
    wd = r"Z:\PhD_Datasets&Analysis\Info_Inputs\TerraClimate\GeoTIFF"
    warmup_yrs = 0 # Number of years to be discarded for the warm-up period
elif working_var in ["bflow3", "wyield3"]:
    wd = r"Z:\PhD_Datasets&Analysis\Outputs\T&M_WBM" + r"\\" + working_var
    working_var = working_var[:-1]  # Remove the last character '3' from the variable name
    warmup_yrs = 6 # Number of years to be discarded for the warm-up period
else:
    wd = r"Z:\PhD_Datasets&Analysis\Outputs\T&M_WBM" + r"\\" + working_var
    warmup_yrs = 6 # Number of years to be discarded for the warm-up period

# Initial year for the analysis
initial_year = 1958 + warmup_yrs

if rangeYears == "":
    # If blank, it is set the default interval
    # Statistics start six years later because of warm-up period
    finalRange = range(initial_year, 2023 + 1)
else:
    splitRange = rangeYears.split("-")
    finalRange = range(int(splitRange[0]), int(splitRange[1]) + 1)


def average_monthly(folder, wv, fr):
    print("")
    print("************************************************************************")
    print("         Averaging " + working_var + " datasets")
    print("************************************************************************")
    print("")

    for month in range(1, 12 + 1):
        print("\tMonth " + str(month))
        out_raster = folder + "\\" + wv + "_month_" + str(month) + wildcard
        rasters = []

        for root, dirnames, filenames in os.walk(folder):
            for year in fr:
                if wildcard == "":
                    for dirname in fnmatch.filter(dirnames, wv + "_" + str(year) + "_" + str(month)):
                        rasters.append(os.path.join(root, dirname))
                else:
                    for filename in fnmatch.filter(filenames, wv + "_" + str(year) + "_" + str(month) + wildcard):
                        rasters.append(os.path.join(root, filename))

        print(rasters)
        out_cell_statistics = CellStatistics(rasters, "MEAN", "DATA")
        out_cell_statistics.save(out_raster)


def generate_annual(folder, wv, fr):
    print("")
    print("************************************************************************")
    print("         Generating " + working_var + " datasets")
    print("************************************************************************")
    print("")

    # Different math operation for temperatures
    if wv[0] == 't':
        operation = "MEAN"
    else:
        operation = "SUM"

    for year in fr:
        print("\tYear " + str(year))
        out_raster = folder + "\\" + wv + "_year_" + str(year) + wildcard

        if wildcard == "":
            allrasters = glob.glob(folder + "\\" + wv + "_" + str(year) + "_*\\")
            #allrasters = glob.glob(folder + "\\" + str(year) + "\\" + wv + "_" + str(year) + "_*\\")
            rasters = []
            for raster in allrasters:
                rasters.append(raster[:-1])
        else:
            rasters = glob.glob(folder + "\\" + wv + "_" + str(year) + "_*" + wildcard)
            #rasters = glob.glob(folder + "\\" + str(year) + "\\" + wv + "_" + str(year) + "_*" + wildcard)
        
        print(rasters)
        out_cell_statistics = CellStatistics(rasters, operation, "DATA")
        out_cell_statistics.save(out_raster)


def average_annual(folder, wv, fr):
    print("")
    print("************************************************************************")
    print("         Averaging " + working_var + " datasets")
    print("************************************************************************")
    print("")

    out_raster = folder + "\\" + wv + "_annual" + wildcard
    rasters = []
    for root, dirnames, filenames in os.walk(folder):
        for year in fr:
            if wildcard == "":
                for dirname in fnmatch.filter(dirnames, wv + "_year_" + str(year)):
                    rasters.append(os.path.join(root, dirname))
            else:
                for filename in fnmatch.filter(filenames,  wv + "_year_" + str(year) + wildcard):
                    rasters.append(os.path.join(root, filename))

    print(rasters)
    out_cell_statistics = CellStatistics(rasters, "MEAN", "DATA")
    out_cell_statistics.save(out_raster)

###########################################################

average_monthly(wd, working_var, finalRange)
generate_annual(wd, working_var, finalRange)
average_annual(wd, working_var, finalRange)

arcpy.CheckInExtension("spatial")

print("DONE!!!")
