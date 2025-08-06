import pandas as pd
import multiprocessing as mp
from multiprocessing import Pool

def setup_arcpy_environment():
    """
    Set up arcpy environment within each worker process.
    This ensures each process has its own independent arcpy instance.
    """
    import arcpy
    from arcpy import env
    
    # Check out spatial analyst extension
    if arcpy.CheckExtension("spatial") == "Available":
        arcpy.CheckOutExtension("spatial")
    else:
        raise Exception("Spatial Analyst extension not available")
    
    # Set arcpy environment variables for this process
    env.parallelProcessingFactor = "80%" # 100% uses all available cores. Still not sure if this is needed
    env.overwriteOutput = True
    env.cellSize = r"Z:\PhD_Datasets&Analysis\Info_Inputs\TerraClimate\GeoTIFF\ppt_2023_1.tif" # Use TerraClimate resolution as reference for cell size
    env.workspace = r"Z:\PhD_Datasets&Analysis\_ProcessingCache"
    env.outputCoordinateSystem = arcpy.SpatialReference("WGS 1984") # WGS 1984 (4326)
    
    return arcpy

def worker_process_year(args):
    """
    Worker function to process a single year. Each process imports arcpy independently.
    Implements memory management to handle large datasets.
    
    Args:
        args: Tuple containing (year, sts_ids, wb_var, tc_vars, processing_dir, out_geotiff, raster_dir, serial_id)
    """
    import gc  # Import garbage collector
    year, sts_ids, wb_var, tc_vars, processing_dir, out_geotiff, raster_dir, serial_id = args
    
    # Import arcpy within the worker process
    arcpy = setup_arcpy_environment()
    
    try:
        print(f"\n**Worker process executing calculation for {year}**")
        
        # Process data in chunks to manage memory
        chunk_size = 10  # Process 10 stations at a time
        months = range(1, 13)  # 1 to 12
        total_stations = len(sts_ids)
        all_dataframes = []
        
        print(f"\tCalculating zonal statistics of '{wb_var}' for year {year}......")
        print(f"\tProcessing {total_stations} stations in chunks of {chunk_size}")
        
        # Process stations in chunks
        for chunk_start in range(0, total_stations, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_stations)
            chunk_stations = sts_ids[chunk_start:chunk_end]
            chunk_dataframes = []
            
            print(f"\t\tYear {year} - Processing stations {chunk_start+1}-{chunk_end} of {total_stations}")
            
            for st in chunk_stations:
                station_dataframes = []
                
                for month in months:
                    try:
                        if wb_var not in tc_vars:
                            # If the variable is not in TerraClimate variables, use the processed variable from the T&M model
                            processing_var = f"{processing_dir}\\{wb_var}_{year}_{month}.tif"
                        else:
                            # If the variable is in TerraClimate variables, use the original GeoTIFF from TerraClimate
                            processing_var = f"{out_geotiff}\\{wb_var}_{year}_{month}.tif"
                        
                        out_table = f"in_memory\\zonal_{wb_var}_{st}_{year}_{month}"
                        
                        # Perform zonal statistics
                        arcpy.sa.ZonalStatisticsAsTable(
                            f"{raster_dir}\\{st}_DA.tif", 
                            "Value", 
                            processing_var, 
                            out_table, 
                            "DATA", 
                            "MEAN"
                        )
                        
                        # Convert to pandas DataFrame
                        array = arcpy.da.TableToNumPyArray(out_table, ["Value", "COUNT", "MEAN"])
                        df_sim = pd.DataFrame(array)
                        df_sim["YEAR"] = year
                        df_sim["MONTH"] = month
                        df_sim.rename(columns={"Value": serial_id}, inplace=True)
                        df_sim = df_sim[[serial_id, "YEAR", "MONTH", "COUNT", "MEAN"]]
                        
                        station_dataframes.append(df_sim)
                        
                        # Clean up immediately
                        arcpy.Delete_management(out_table)
                        del array, df_sim  # Explicitly delete variables
                        
                    except Exception as month_error:
                        print(f"\t\t\tWarning: Error processing station {st}, month {month}: {str(month_error)}")
                        continue
                
                # Combine station data and add to chunk
                if station_dataframes:
                    station_combined = pd.concat(station_dataframes, ignore_index=True)
                    chunk_dataframes.append(station_combined)
                    del station_dataframes, station_combined  # Clean up
            
            # Combine chunk data
            if chunk_dataframes:
                chunk_combined = pd.concat(chunk_dataframes, ignore_index=True)
                all_dataframes.append(chunk_combined)
                del chunk_dataframes, chunk_combined
            
            # Force garbage collection after each chunk
            gc.collect()
            
            # Clear ArcGIS in-memory workspace periodically
            try:
                arcpy.Delete_management("in_memory")
            except:
                pass
        
        # Combine all data
        if all_dataframes:
            sts_flows_sim = pd.concat(all_dataframes, ignore_index=True)
            del all_dataframes  # Clean up
        else:
            sts_flows_sim = pd.DataFrame(columns=[serial_id, "YEAR", "MONTH", "COUNT", "MEAN"])
        
        print(f"\tYear {year} - Saving zonal statistics results into CSV......")
        
        # Save results
        if wb_var not in tc_vars:
            output_path = f"{processing_dir}\\{wb_var}_zonal_statistics_{year}.csv"
        else:
            output_path = f"{out_geotiff}\\{wb_var}_zonal_statistics_{year}.csv"
        
        sts_flows_sim.to_csv(output_path, index=False)
        del sts_flows_sim  # Clean up final dataframe
        
        # Final cleanup and garbage collection
        gc.collect()
        
        print(f"\tYear {year} - Processing completed successfully!")
        return f"Year {year} completed successfully"
        
    except Exception as e:
        print(f"Error processing year {year}: {str(e)}")
        # Try to free up memory even on error
        try:
            arcpy.Delete_management("in_memory")
            gc.collect()
        except:
            pass
        return f"Year {year} failed: {str(e)}"
    
    finally:
        # Clean up arcpy resources and force garbage collection
        try:
            arcpy.Delete_management("in_memory")
            arcpy.CheckInExtension("spatial")
            arcpy.ClearEnvironment("workspace")
            gc.collect()
        except:
            pass

if __name__ == "__main__":
    # Paths to input datasets (keep these in main process)
    root_folder = r"Z:\PhD_Datasets&Analysis\Info_Inputs"
    css_folder = root_folder + "\\Streamflow_Sts_Drainage_Areas\GRDC_Watersheds"
    tam_out_dir = r"Z:\PhD_Datasets&Analysis\Outputs\T&M_WBM"
    tc_ds = root_folder + "\\TerraClimate"
    out_geotiff = tc_ds + "\\GeoTIFF"
    serial_id = 'grdcno_int'
    tc_vars = ["ppt", "pet", "q"] # Variable names according to TerraClimate
    wb_var = 'wyield2' # Example variable, can be changed as needed
    
    # Set up arcpy environment for main process (only for reading station IDs)
    import arcpy
    from arcpy import env
    
    if arcpy.CheckExtension("spatial") == "Available":
        arcpy.CheckOutExtension("spatial")
    else:
        print("Spatial Analyst extension not available")
    
    env.overwriteOutput = True
    env.workspace = r"Z:\PhD_Datasets&Analysis\_ProcessingCache"
    env.outputCoordinateSystem = arcpy.SpatialReference("WGS 1984")
    
    # Get spatial reference info
    spatial_ref = env.outputCoordinateSystem
    if spatial_ref:
        print(f"Spatial Reference Name: {spatial_ref.name}")
        print(f"Spatial Reference WKID: {spatial_ref.factoryCode}")
    else:
        print("No spatial reference is set in the current environment.")
    
    # Read station IDs
    drain_areas = root_folder + "\\Streamflow_Sts_Drainage_Areas\GRDC_Watersheds\CSS-WATERSHEDS-MERGE_FINAL_SELECTION.shp"
    arcpy.MakeFeatureLayer_management(drain_areas, "drain_areas_lyr")
    
    sts_ids = []
    sql_field = "has_monthl" # Field to filter rows with monthly k recessions
    #sql_field = "has_daily_" # Field to filter rows with daily k recessions
    sql_query = f'{sql_field} = \'Yes\''
    
    with arcpy.da.SearchCursor("drain_areas_lyr", [serial_id], sql_query) as cursor:
        for row in cursor:
            sts_ids.append(row[0])
    
    print(f"Station IDs: {sts_ids}")
    print(f"Total stations: {len(sts_ids)}")
    
    # Clean up main process arcpy
    arcpy.CheckInExtension("spatial")
    arcpy.ClearEnvironment("workspace")
    
    # Prepare variables for parallel processing
    years = range(1958, 2024)  # Years to process
    processing_dir = tam_out_dir + '\\' + wb_var
    
    if wb_var in ["bflow2", "wyield2"]:
        wb_var = wb_var[:-1]  # Remove the last character '2'
    
    raster_dir = css_folder + '\\Final_Rasters'
    
    print('\n############################################################')
    print('\t\tINITIAL VARIABLES')
    print('\tPeriod to be executed: ' + str(years[0]) + '-' + str(years[-1]))
    print('\tNumber of stations: ' + str(len(sts_ids)))
    print('\tVariable: ' + wb_var)
    print('############################################################')
    
    # Prepare arguments for each worker process
    worker_args = []
    for year in years:
        args = (year, sts_ids, wb_var, tc_vars, processing_dir, out_geotiff, raster_dir, serial_id)
        worker_args.append(args)
    
    # Determine number of processes to use (limit to avoid handle overflow)
    max_processes = min(8, mp.cpu_count() - 1)  # Limit to 8 processes to avoid handle issues
    batch_size = max_processes  # Process in batches
    
    print(f"\nUsing {max_processes} parallel processes")
    print(f"Processing {len(years)} years in batches of {batch_size}")
    
    # Execute parallel processing in batches
    print("\nStarting parallel processing...")
    
    all_results = []
    
    # Process years in batches to avoid handle limit
    for i in range(0, len(worker_args), batch_size):
        batch_args = worker_args[i:i + batch_size]
        batch_years = [args[0] for args in batch_args]  # Extract years for logging
        
        print(f"\nProcessing batch: years {batch_years[0]} to {batch_years[-1]} ({len(batch_args)} years)")
        
        with Pool(processes=min(max_processes, len(batch_args))) as pool:
            batch_results = pool.map(worker_process_year, batch_args)
        
        all_results.extend(batch_results)
        print(f"Completed batch: years {batch_years[0]} to {batch_years[-1]}")
    
    results = all_results
    
    # Print results
    print("\n" + "="*60)
    print("PARALLEL PROCESSING RESULTS:")
    print("="*60)
    for result in results:
        print(result)
    
    print("\nALL PROCESSING COMPLETED!!")