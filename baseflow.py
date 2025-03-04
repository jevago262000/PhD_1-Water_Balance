import numpy as np
import os

def main():
    """
    Main program to estimate groundwater contributions from USGS streamflow records.
    Uses a recursive filter technique to separate base flow and calculate
    the streamflow recession constant (alpha).
    """

    # Get user input for paths
    try:
        input_dir = raw_input("Enter the path to the input files: ").strip()  # Python 2
        output_dir = raw_input("Enter the path to save output files: ").strip()  # Python 2
    except NameError:
        input_dir = input("Enter the path to the input files: ").strip()  # Python 3
        output_dir = input("Enter the path to save output files: ").strip()  # Python 3

    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    # Initialize universal variables
    ndmin = 0
    ndmax = 0
    iprint = 0

    # Define paths for files
    baseflow_file = os.path.join(output_dir, "baseflow.dat")
    input_list_file = os.path.join(input_dir, "file.lst")
    
    # Open output files
    with open(baseflow_file, 'w') as outfile:
        outfile.write("Baseflow data file: this file summarizes the fraction of streamflow that is contributed by baseflow for each of the 3 passes made by the program\n\n")
        outfile.write("Gage file      Baseflow Fr1 Baseflow Fr2 Baseflow Fr3   NPR Alpha Factor Baseflow Days\n")
    
    # Process input file
    with open(input_list_file, 'r') as infile:
        # Process preliminary information
        titldum = infile.readline()  # Skip first line
        ndmin = int(infile.readline().split()[0])
        ndmax = int(infile.readline().split()[0])
        iprint = int(infile.readline().split()[0])
        titldum = infile.readline()  # Skip line
        titldum = infile.readline()  # Skip line
        
        # Process USGS data files
        for line in infile:
            if not line.strip():
                continue
            
            parts = line.strip().split()
            if len(parts) >= 2:
                flwfile_name = parts[0]
                flwfileo_name = parts[1]
            else:
                continue
            
            # Construct full file paths
            flwfile = os.path.join(input_dir, str(flwfile_name).lower())
            flwfileo = os.path.join(output_dir, str(flwfileo_name).lower())
            
            # Find out number of years of stream gage data
            with open(flwfile, 'r') as datafile:
                ndays = sum(1 for _ in datafile) - 1  # Subtract header line
            
            if ndays < 2:
                print("Error: No data in file, stopping program")
                return
            
            # Initialize arrays
            mon = np.zeros(ndays, dtype=int)
            iday = np.zeros(ndays, dtype=int)
            jday = np.zeros(ndays, dtype=int)
            iyr = np.zeros(ndays, dtype=int)
            strflow = np.zeros(ndays)
            surfq = np.zeros(ndays)
            baseq = np.zeros((3, ndays))
            
            # Read in streamflow data
            with open(flwfile, 'r') as datafile:
                titldum = datafile.readline()  # Skip header
                isumd = 0
                
                for line in datafile:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        date = int(parts[0])
                        sflow = float(parts[1])
                        
                        if sflow > 9998:
                            sflow = 0.0
                        
                        iyr[isumd] = date // 10000
                        date = date - (iyr[isumd] * 10000)
                        mon[isumd] = date // 100
                        iday[isumd] = date - (mon[isumd] * 100)
                        
                        # Calculate Julian day
                        leapyr = 0
                        if iyr[isumd] % 4 != 0:
                            leapyr = 1
                        
                        if mon[isumd] == 1:
                            jday[isumd] = iday[isumd]
                        elif mon[isumd] == 2:
                            jday[isumd] = 31 + iday[isumd]
                        elif mon[isumd] == 3:
                            jday[isumd] = 60 + iday[isumd] - leapyr
                        elif mon[isumd] == 4:
                            jday[isumd] = 91 + iday[isumd] - leapyr
                        elif mon[isumd] == 5:
                            jday[isumd] = 121 + iday[isumd] - leapyr
                        elif mon[isumd] == 6:
                            jday[isumd] = 152 + iday[isumd] - leapyr
                        elif mon[isumd] == 7:
                            jday[isumd] = 182 + iday[isumd] - leapyr
                        elif mon[isumd] == 8:
                            jday[isumd] = 213 + iday[isumd] - leapyr
                        elif mon[isumd] == 9:
                            jday[isumd] = 244 + iday[isumd] - leapyr
                        elif mon[isumd] == 10:
                            jday[isumd] = 274 + iday[isumd] - leapyr
                        elif mon[isumd] == 11:
                            jday[isumd] = 305 + iday[isumd] - leapyr
                        elif mon[isumd] == 12:
                            jday[isumd] = 335 + iday[isumd] - leapyr
                        
                        strflow[isumd] = sflow
                        isumd += 1
            
            # Perform passes to calculate base flow
            f1 = 0.925
            f2 = 0.0
            f2 = (1.0 + f1) / 2.0
            surfq[0] = strflow[0] * 0.5
            baseq[0, 0] = strflow[0] - surfq[0]
            baseq[1, 0] = baseq[0, 0]
            baseq[2, 0] = baseq[0, 0]
            
            # Make the first pass (forward)
            for i in range(1, isumd):
                surfq[i] = f1 * surfq[i-1] + f2 * (strflow[i] - strflow[i-1])
                if surfq[i] < 0:
                    surfq[i] = 0
                baseq[0, i] = strflow[i] - surfq[i]
                if baseq[0, i] < 0:
                    baseq[0, i] = 0
                if baseq[0, i] > strflow[i]:
                    baseq[0, i] = strflow[i]
            
            # Make the second pass (backward)
            baseq[1, isumd-2] = baseq[0, isumd-2]
            for i in range(isumd-3, -1, -1):
                surfq[i] = f1 * surfq[i+1] + f2 * (baseq[0, i] - baseq[0, i+1])
                if surfq[i] < 0:
                    surfq[i] = 0
                baseq[1, i] = baseq[0, i] - surfq[i]
                if baseq[1, i] < 0:
                    baseq[1, i] = 0
                if baseq[1, i] > baseq[0, i]:
                    baseq[1, i] = baseq[0, i]
            
            # Make the third pass (forward)
            baseq[2, isumd-2] = baseq[0, isumd-2]
            for i in range(1, isumd):
                surfq[i] = f1 * surfq[i-1] + f2 * (baseq[1, i] - baseq[1, i-1])
                if surfq[i] < 0:
                    surfq[i] = 0
                baseq[2, i] = baseq[1, i] - surfq[i]
                if baseq[2, i] < 0:
                    baseq[2, i] = 0
                if baseq[2, i] > baseq[1, i]:
                    baseq[2, i] = baseq[1, i]
            
            # Perform summary calculations
            sumbf1 = np.sum(baseq[0, :])
            sumbf2 = np.sum(baseq[1, :])
            sumbf3 = np.sum(baseq[2, :])
            sumstrf = np.sum(strflow)
            
            # Calculate baseflow fractions
            bflw_fr1 = sumbf1 / sumstrf
            bflw_fr2 = sumbf2 / sumstrf
            bflw_fr3 = sumbf3 / sumstrf
            
            # Compute streamflow recession constant (alpha)
            nregmx = 300
            
            # Initialize arrays
            alpha = np.zeros(ndays)
            ndreg = np.zeros(ndays, dtype=int)
            q0 = np.zeros(ndays)
            q10 = np.zeros(ndays)
            
            # Initialize variables
            aveflo = np.zeros(nregmx)
            florec = np.zeros((nregmx, 200))
            npreg = np.zeros(nregmx, dtype=int)
            idone = np.zeros(nregmx, dtype=int)
            bfdd = np.zeros(nregmx)
            qaveln = np.zeros(nregmx)
            
            nd = 0
            for i in range(1, ndays):
                if strflow[i-1] <= 0:
                    nd = 0
                else:
                    if baseq[0, i-1] / strflow[i-1] < 0.99:
                        if nd >= ndmin:
                            alpha[i-1] = np.log(strflow[i-1-nd] / strflow[i-2]) / nd
                            ndreg[i-1] = nd
                        nd = 0
                    else:
                        nd += 1
                        if nd >= ndmax:
                            alpha[i-1] = np.log(strflow[i-nd] / strflow[i-1]) / nd
                            ndreg[i-1] = nd
                            nd = 0
            
            npr = 0
            for i in range(ndays):
                # Compute x and y coords for alpha regression analysis
                if alpha[i] > 0:
                    if mon[i] <= 2 or mon[i] >= 11:
                        npr += 1
                        q10[npr-1] = strflow[i-1]
                        q0[npr-1] = strflow[i-ndreg[i]]
                        if q0[npr-1] - q10[npr-1] > 0.001:
                            bfdd[npr-1] = ndreg[i] / (np.log(q0[npr-1]) - np.log(q10[npr-1]))
                            qaveln[npr-1] = np.log((q0[npr-1] + q10[npr-1]) / 2)
                            kk = 0
                            for k in range(1, ndreg[i]+1):
                                x = np.log(strflow[i-k])
                                if x > 0:
                                    florec[kk, npr-1] = x
                                    kk += 1
                            if kk == 0:
                                npr -= 1
            
            # Estimate master recession curve
            if npr > 1:
                icount = np.zeros(200, dtype=int)
                
                np_count = npr
                sumx = np.sum(qaveln[:npr])
                sumy = np.sum(bfdd[:npr])
                sumxy = np.sum(qaveln[:npr] * bfdd[:npr])
                sumx2 = np.sum(qaveln[:npr] * qaveln[:npr])
                
                ssxy = sumxy - (sumx * sumy) / np_count
                ssxx = sumx2 - (sumx * sumx) / np_count
                slope = ssxy / ssxx
                yint = sumy / np_count - slope * sumx / np_count
                
                # Find the recession curve with the lowest point on it
                for j in range(npr):
                    amn = 1.0e20
                    now = 0
                    
                    for i in range(npr):
                        if idone[i] == 0:
                            if florec[0, i] < amn:
                                amn = florec[0, i]
                                now = i
                    
                    idone[now] = 1
                    
                    # Now is the number in array florec of the current smallest flow
                    # icount keeps track of where the now recession curve falls on the
                    # x axis (day line)
                    
                    igap = 0
                    if j == 0:
                        icount[now] = 1
                        igap = 1
                    else:
                        for i in range(nregmx):
                            if florec[0, now] <= aveflo[i]:
                                icount[now] = i
                                igap = 1
                                break
                    
                    # If there is a gap, run linear regression on the average flow
                    if igap == 0:
                        np_count = 0
                        sumx = 0
                        sumy = 0
                        sumxy = 0
                        sumx2 = 0
                        
                        for i in range(nregmx):
                            if aveflo[i] > 0:
                                np_count += 1
                                x = float(i)
                                sumx += x
                                sumy += aveflo[i]
                                sumxy += x * aveflo[i]
                                sumx2 += x * x
                        
                        if sumx > 1:
                            ssxy = sumxy - (sumx * sumy) / np_count
                            ssxx = sumx2 - (sumx * sumx) / np_count
                            slope = ssxy / ssxx
                            yint = sumy / np_count - slope * sumx / np_count
                            icount[now] = int((florec[0, now] - yint) / slope)
                        else:
                            slope = 0
                            yint = 0
                            icount[now] = 0
                    
                    # Update average flow array
                    for i in range(ndmax):
                        if florec[i, now] > 0.0001:
                            k = icount[now] + i - 1
                            aveflo[k] = (aveflo[k] * npreg[k] + florec[i, now]) / (npreg[k] + 1)
                            if aveflo[k] <= 0:
                                aveflo[k] = slope * i + yint
                            npreg[k] = npreg[k] + 1
                        else:
                            break
                
                # Run alpha regression on all adjusted points
                # Calculate alpha factor for groundwater
                np_count = 0
                sumx = 0
                sumy = 0
                sumxy = 0
                sumx2 = 0
                
                for j in range(npr):
                    for i in range(ndmax):
                        if florec[i, j] > 0:
                            np_count += 1
                            x = float(icount[j] + i)
                            sumx += x
                            sumy += florec[i, j]
                            sumxy += x * florec[i, j]
                            sumx2 += x * x
                        else:
                            break
                
                ssxy = sumxy - (sumx * sumy) / np_count
                ssxx = sumx2 - (sumx * sumx) / np_count
                alf = ssxy / ssxx
                bfd = 2.3 / alf
                
                with open(baseflow_file, 'a') as outfile:
                    outfile.write("{:<15} {:>12.2f} {:>12.2f} {:>12.2f} {:>6d} {:>12.4f} {:>13.4f}\n".format(
                        flwfile_name, bflw_fr1, bflw_fr2, bflw_fr3, npr, alf, bfd))
            else:
                with open(baseflow_file, 'a') as outfile:
                    outfile.write("{:<15} {:>12.2f} {:>12.2f} {:>12.2f}\n".format(
                        flwfile_name, bflw_fr1, bflw_fr2, bflw_fr3))
            
            # If daily baseflow values are wanted
            if iprint == 1:
                with open(flwfileo, 'w') as outfile:
                    outfile.write("Daily baseflow filters values for data from: {}\n".format(flwfile_name))
                    outfile.write("YEARMNDY   Streamflow  Bflow Pass1  Bflow Pass2  Bflow Pass3\n")
                    
                    for i in range(ndays):
                        outfile.write("{:4d}{:2d}{:2d} {:12.6e} {:12.6e} {:12.6e} {:12.6e}\n".format(
                            iyr[i], mon[i], iday[i], strflow[i], baseq[0, i], baseq[1, i], baseq[2, i]))

if __name__ == "__main__":
    main()