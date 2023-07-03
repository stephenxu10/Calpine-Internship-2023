"""
Joshua Xia - joshua_x@tamu.edu, joshuaxia7@gmail.com
Last Edited: 6/30/23
"""
from gridworkbench import GridWorkbench
import numpy as np
from math import tan, pi, exp, atan, log, sqrt, cos, sin
import manualOpenPWB
import pandas as pd
print("Started CRR program")
wb = GridWorkbench()
wb.bus_pw_fields.append(("zone_name", "ZoneName", 0, lambda x: x))  # add field zone_name to bus objects
fileName = r"Y:\11_Transmission Analysis\CAISO\10 - Studies\2023\03-CRR\Monthly\Mn-07\MN_JUL_2023_T1_DB120_V2\APSSE20230531120002_SUMMER_W_ITC.raw"
wb.open_pwb(fileName)
wb.pwb_read_all(hush = True)

# Build busBranchDict
busBranchDict = {}  # bus number : branch array
for bus in wb.buses:
    branchList = []
    if bus.nominal_kv >= 500 and bus.zone_name == "PGAE-30":
        for branch in bus.branches:
            if branch.to_bus.nominal_kv >= 500 and branch.from_bus.nominal_kv >= 500 and branch.to_bus.zone_name == "PGAE-30" and branch.from_bus.zone_name == "PGAE-30":    # check for legal branches
                branchList.append(branch)
        branchList = list(set(branchList))  # get rid of duplicates
        busBranchDict[bus] = branchList  # keys are bus objects

# Build CRRLines list
CRRLines = []
CRRDict = {}
for key in busBranchDict.keys():
    branchArray  = busBranchDict[key]
    branchString = key.name + ":"
    for branch in branchArray:
        branchString = branchString + str(branch.from_bus.number) + "," + str(branch.to_bus.number)  + "," + str(branch.from_bus.name) + "," + str(branch.to_bus.name) + "/"
    branchString = branchString[ :  -1]
    CRRLines.append(branchString)
CRRDict["BusBranchDict[Bus Name] = Branch FromBusNumber ToBusNumber CircuitNumber FromBusName ToBusName/etc"] = CRRLines
df = pd.DataFrame(CRRDict)
"""
for line in CRRLines:
    """"""
    fromNumbersList = []
    toNumbersList = []
    fromNamesList = []
    toNamesList = []
    fromToNames = []
    indexOfColon = line.index(":")
    busKey = line[ : indexOfColon]

    secondHalf = line[ (indexOfColon + 1) : ]
    splittedByDash = secondHalf.split("/")

    print("line =", line)
    print("splitted =", splittedByDash)
    for branch in splittedByDash:
        splittedByComma = branch.split(",")
        fromNumbersList.append(splittedByComma[0])
        toNumbersList.append(splittedByComma[1])
        fromNamesList.append(splittedByComma[2])
        toNamesList.append(splittedByComma[3] )
        print("splittedByComma =", splittedByComma)
    break
"""
# Map the bus names and numbers in the two cases
"""
CRRToWECCDict = {}  # "{  CRRName,CRRNumber : [WECC Name, WECC Number], etc  }
for key in busBranchDict.keys():
    isInBothPFCases = False
    for key2 in manualOpenPWB.busBranchDict.keys(): # check if the key is in both PF cases
        if key == key2:
            isInBothPFCases = True
    if isInBothPFCases: # only process keys that are in both PF cases
        for CRRbranch in busBranchDict[key]:    # iterate through CRR branches
            for WECCbranch in manualOpenPWB.busBranchDict[key]:  # iterate through WECC case
                CRRFromNumber = CRRbranch.from_bus.number
                CRRToNumber = CRRbranch.to_bus.number
                CRRFromName = CRRbranch.from_bus.name
                CRRToName = CRRbranch.to_bus.name

                WECCFromNumber = WECCbranch.from_bus.number
                WECCToNumber = WECCbranch.to_bus.number
                WECCFromName = WECCbranch.from_bus.name
                WECCToName = WECCbranch.to_bus.name

                print("CRRFromNumber =", CRRFromNumber, "WECCFromNumber =" ,WECCFromNumber)
"""

# Write to Excel
with pd.ExcelWriter(r"Y:\11_Transmission Analysis\ERCOT\101 - Misc\CRR Limit Aggregates\Network Topology\CRR Bus and Branches.xlsx") as writer:
        df.to_excel(writer, sheet_name = "Branch Strings", index = False)



