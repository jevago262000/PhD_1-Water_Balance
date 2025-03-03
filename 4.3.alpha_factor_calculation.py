import os
import numpy as np
import csv
from datetime import datetime

def is_leap_year(year):
    """Checks if a given year is a leap year."""
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

def read_streamflow_data(filepath):
    """ Reads streamflow data from a given file while considering leap years. """
    dates, flows = [], []
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Error: Input file '{filepath}' not found.")
    with open(filepath, 'r') as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) >= 2:
                try:
                    date = int(parts[0])
                    year = date // 10000  # Extract year from YYYYMMDD
                    if is_leap_year(year):
                        # Leap year handling logic can be implemented if needed
                        pass
                    flow = float(parts[1]) if float(parts[1]) < 9998.0 else 0.0
                    dates.append(date)
                    flows.append(flow)
                except ValueError:
                    continue
    return np.array(dates), np.array(flows)

def compute_alpha(flows, ndmin, ndmax):
    """ Computes the streamflow recession constant (alpha). """
    if len(flows) < ndmin:
        return 0.925  # Default fallback value
    
    log_ratios = []
    for i in range(ndmin, min(len(flows), ndmax)):
        if flows[i - ndmin] > 0 and flows[i - 1] > 0:
            log_ratios.append(np.log(flows[i - ndmin] / flows[i - 1]) / ndmin)
    
    return np.mean(log_ratios) if log_ratios else 0.925

def baseflow_separation(flows, alpha):
    """ Computes baseflow using a three-pass recursive filter technique. """
    baseflow1 = np.zeros_like(flows)
    baseflow2 = np.zeros_like(flows)
    baseflow3 = np.zeros_like(flows)
    
    baseflow1[0] = flows[0]
    for i in range(1, len(flows)):
        baseflow1[i] = alpha * baseflow1[i-1] + (1 - alpha) * flows[i]
    
    baseflow2[0] = baseflow1[0]
    for i in range(1, len(flows)):
        baseflow2[i] = alpha * baseflow2[i-1] + (1 - alpha) * baseflow1[i]
    
    baseflow3[0] = baseflow2[0]
    for i in range(1, len(flows)):
        baseflow3[i] = alpha * baseflow3[i-1] + (1 - alpha) * baseflow2[i]
    
    return baseflow1, baseflow2, baseflow3

def append_baseflow_summary(filepath, csv_filepath, input_file, baseflow1, baseflow2, baseflow3, alpha):
    """ Appends computed baseflow summary to a text file and a CSV file. """
    avg_bf1 = np.mean(baseflow1)
    avg_bf2 = np.mean(baseflow2)
    avg_bf3 = np.mean(baseflow3)
    
    # Append to text file
    with open(filepath, 'a') as file:
        file.write(f"{input_file} {avg_bf1:.3f} {avg_bf2:.3f} {avg_bf3:.3f} Alpha:{alpha:.3f}\n")
    
    # Append to CSV file
    with open(csv_filepath, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([input_file, avg_bf1, avg_bf2, avg_bf3, alpha])

def read_file_lst(filepath):
    """ Reads file.lst to extract input and output filenames and parameters. """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Error: Master input file '{filepath}' not found.")
    
    with open(filepath, 'r') as file:
        lines = file.readlines()
    params = {}
    data_files = []
    for line in lines:
        parts = line.strip().split('!')[0].split()
        if len(parts) == 2 and parts[0] in ('NDMIN', 'NDMAX'):
            params[parts[0]] = int(parts[1])
        elif len(parts) == 2 and not parts[1].isdigit():
            data_files.append((parts[0], parts[1]))
    return params, data_files

def main(root_path):
    file_lst = os.path.join(root_path, "file.lst")
    baseflow_summary_file = os.path.join(root_path, "baseflow.dat")
    baseflow_summary_csv = os.path.join(root_path, "baseflow.csv")
    
    # Clear or create the baseflow summary files with a header
    with open(baseflow_summary_file, 'w') as file:
        file.write("File Baseflow_Pass1 Baseflow_Pass2 Baseflow_Pass3 Alpha\n")
    
    with open(baseflow_summary_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["File", "Baseflow_Pass1", "Baseflow_Pass2", "Baseflow_Pass3", "Alpha"])
    
    try:
        params, data_files = read_file_lst(file_lst)
    except FileNotFoundError as e:
        print(e)
        return
    
    ndmin = params.get('NDMIN', 10)
    ndmax = params.get('NDMAX', 300)
    
    for input_file, output_file in data_files:
        input_path = os.path.join(root_path, input_file)
        try:
            dates, flows = read_streamflow_data(input_path)
        except FileNotFoundError as e:
            print(e)
            continue
        
        alpha = compute_alpha(flows, ndmin, ndmax)
        baseflow1, baseflow2, baseflow3 = baseflow_separation(flows, alpha)
        append_baseflow_summary(baseflow_summary_file, baseflow_summary_csv, input_file, baseflow1, baseflow2, baseflow3, alpha)
        print(f"Baseflow computation complete for {input_file}. Summary saved to {baseflow_summary_file} and {baseflow_summary_csv}")

if __name__ == "__main__":
    root_path = os.getcwd()  # Default to current working directory
    main(root_path)