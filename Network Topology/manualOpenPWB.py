"""
Joshua Xia - joshua_x@tamu.edu, joshuaxia7@gmail.com
Last Edited: 6/30/23
"""
import win32com.client
import os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np

# SimAuto lists and functions
bus_param_list = ['BusNum', 'BusName','AreaNum', "AreaName", "ZoneName", "NomkV"]        #ParamList
branch_param_list = ['BusNumFrom','BusNumTo','BusNameFrom', 'BusNameTo', 'Circuit','Status','BranchDeviceType','LineLimMVA']
gen_param_list = ['BusNum','BusName', 'ID', 'AllLabels','Status','GenMW','GenMWMax']
load_param_list = ['BusNum','BusName', 'ID', 'AreaNum','Status','LoadMW']
def get_bus_branch(filecase, bus_paramter, branch_paramter, bus_filter, branch_filter):
    simauto_obj = win32com.client.Dispatch('pwrworld.SimulatorAuto')
    simauto_obj.OpenCase(filecase)

    bus_df = get_object(simauto_obj, 'Bus', bus_paramter, bus_filter)
    branch_df = get_object(simauto_obj, 'Branch', branch_paramter, branch_filter)

    """
    # Write to Excel
    with pd.ExcelWriter(r"C:/Users\jx17594\Documents\venv\PF Case Matching\WECC Branches.xlsx") as writer:
            branch_df.to_excel(writer, sheet_name= "WECC Branches", index = False)
    """
    return [bus_df, branch_df]
def get_object(simauto_obj, object_type, param_list, filter_name):
    object_tuple = simauto_obj.GetParametersMultipleElementFlatOutput(object_type, param_list, filter_name)
    object_array = np.array(object_tuple[3:]).reshape(-1, len(param_list))
    object_df = pd.DataFrame(object_array, columns=param_list)
    return object_df

# Open case, read in data
filecase = r"Y:\11_Transmission Analysis\03 - Power Flow Cases\WECC\2023 Series\23HS4a1PW\23HS4a1.PWB"
busBranchDF = get_bus_branch(filecase, bus_param_list, branch_param_list,'','')
bus_df = busBranchDF[0]
branches_df = busBranchDF[1]
class branch:
    def __init__(self, busNumFrom, busNumTo, busNameFrom, busNameTo, circuit, branchStatus, branchDeviceType, lineLimMVA):
        busNumDict[busNumFrom].addBranch(self)
        busNumDict[busNumTo].addBranch(self)
        self.from_bus = busNumDict[busNumFrom]
        self.to_bus = busNumDict[busNumTo]
        self.from_bus_name = busNameFrom
        self.to_bus_name = busNameTo
        self.circuit_number = circuit
        self.status = branchStatus
        self.deviceType = branchDeviceType
        self.LineLimit = lineLimMVA
class bus:
    def __init__(self, busNum, busName, areaNum, aName, zoneName, nomKV):
        self.number = int(busNum)
        self.name = busName
        self.areaNumber = areaNum
        self.areaName = aName
        self.zone_name = zoneName
        self.nominal_kv = float(nomKV)
        self.branches = []  # list of branch object
    def addBranch(self, branch):
        self.branches.append(branch)

# Create bus objects and bus number dictionary
busNumDict= {}
busNums = bus_df["BusNum"]
busNames = bus_df["BusName"]
areaNums = bus_df["AreaNum"]
areaNames = bus_df["AreaName"]
zoneNames = bus_df["ZoneName"]
nomKVs = bus_df["NomkV"]
for i in range(len(busNums)):
    busNumber = int(busNums[i])
    busName = busNames[i]
    areaNum = areaNums[i]
    areaName = areaNames[i]
    zoneName = zoneNames[i]
    nomKV = float(nomKVs[i])
    busNumDict[busNumber] = bus(busNumber, busName, areaNum, areaName, zoneName, nomKV) # add the bus to the dictionary

# Create list of branch objects
branchesList = []
fromBusNumbers = list(branches_df["BusNumFrom"])
toBusNumbers = list(branches_df["BusNumTo"])
fromBusNames = list(branches_df["BusNameFrom"])
toBusNames = list(branches_df["BusNameTo"])
circuitNums = list(branches_df["Circuit"])
statuses = list(branches_df["Status"])
branchTypes = list(branches_df["BranchDeviceType"])
lineLimits = list(branches_df["LineLimMVA"])
for i in range(len(fromBusNumbers)):
    fNum = int(fromBusNumbers[i])
    tNum = int(toBusNumbers[i])
    fName = fromBusNames[i]
    tBusName = toBusNames[i]
    circuitNum = circuitNums[i] # could have characters, so it's a string
    stat = statuses[i]
    device = branchTypes[i]
    lineLim = float(lineLimits[i])
    newBranch  = branch(fNum, tNum, fName, tBusName, circuitNum, stat, device, lineLim)
    branchesList.append(newBranch)

# Create bus branch dictionary list to compare
busBranchDict = {}
for key in busNumDict.keys():
    bus = busNumDict[key]
    branchList = []
    if bus.nominal_kv >= 500 and bus.areaName == "PG AND E":
        for branch in bus.branches:
            if branch.to_bus.nominal_kv >= 500 and branch.from_bus.nominal_kv >= 500 and branch.to_bus.areaName == "PG AND E" and branch.from_bus.areaName == "PG AND E":    # check for legal branches
                branchList.append(branch)
        branchList = list(set(branchList))  # get rid of duplicates
        busBranchDict[bus] = branchList # bus objects are keys in the dictionary
# Write to Excel to check
toNumbers_out = []
toNames_out = []
fromNumbers_out = []
fromNames_out = []
outputDict = {}
WECCLines = []

for key in busBranchDict.keys():
    branches =  busBranchDict[key]
    branchString = key.name + ":"
    for branch in branches:
        branchString = branchString + str(branch.from_bus.number) + "," + str(branch.to_bus.number)  + "," + str(branch.from_bus.name) + "," + str(branch.to_bus.name) + "/"
    branchString = branchString[:  -1]
    WECCLines.append(branchString)
WECCDict = {}
WECCDict["BusBranchDict[Bus Name] = Branch FromBusNumber ToBusNumber CircuitNumber FromBusName ToBusName/etc"] = WECCLines
df = pd.DataFrame(WECCDict)
# Write to Excel
with pd.ExcelWriter(r"Y:\11_Transmission Analysis\ERCOT\101 - Misc\CRR Limit Aggregates\Network Topology\WECC Bus and Branches.xlsx") as writer:
        df.to_excel(writer, sheet_name = "Branch Strings", index = False)
print("Finished running WECC program")
