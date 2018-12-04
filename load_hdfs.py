import os
import time
import subprocess
from collections import defaultdict

# define flie locations
hdfsPath = '/data/'
localPath = '/root/final_projects/us/'
irsPath = '/root/final_projects/16zpallagi.csv' # includes file name

# define state list
statesCap = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
states = [x.lower() for x in statesCap]

# define HDFS command function
def run_cmd(args_list, verbose = False):
    if verbose == True:
        print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err

# build directry structre in HDFS
start = time.time()
stateFiles = 0
(ret, out, err) = run_cmd(['hdfs', 'dfs', '-mkdir', hdfsPath])
for state in states:
    location = hdfsPath + state + '/'
    (ret, out, err) = run_cmd(['hdfs', 'dfs', '-mkdir', location])
    stateFiles +=1
print("Created {} subfolders in hdfs dfs {}".format(stateFiles, hdfsPath))
print("Directory took {} minutes".format(round((time.time()-start)/60,2)))

# upload IRS data file
(ret, out, err) = run_cmd(['hdfs', 'dfs', '-put', irsPath, hdfsPath])
print("Added IRS data file to {}".format(hdfsPath))

start = time.time()
totFiles = 0
stateSummary = defaultdict(int)

# get county name list of .csv files
for state in states:
    # define state file path
    os.chdir(localPath + state + '/')
    # get list of county files
    counties = [doc for doc in os.listdir() if doc.endswith((".csv"))]

    for file in counties:
        # define file location
        hdfsLoc = hdfsPath + state + '/' + file
        fileLoc = localPath + state + '/' + file

        # Run Hadoop put command in Python
        (ret, out, err) = run_cmd(['hdfs', 'dfs', '-put', fileLoc, hdfsLoc])
        totFiles += 1
        stateSummary[state] += 1

    print("Completed {}".format(state))
    #print(counties)
    # path = "/root/final_projects/us"os.chdir(path)YY = [doc for doc in os.listdir() if (doc.endswith((".csv"))


print("Files loaded in {} minutes".format(round((time.time()-start)/60,2)))
print("Total files:  {}".format(totFiles))
print("State Check:  {}.  Should be 51".format(len(stateSummary)))
print("Files by State:  {}".format(stateSummary))
