import win32com.client
import os
import pandas as pd
import numpy as np

bus_param_list = ['BusNum', 'BusName', 'AreaNum', 'NomkV']  # ParamList
branch_param_list = ['BusNumFrom', 'BusNumTo', 'Circuit', 'Status', 'BranchDeviceType', 'LineLimMVA']
gen_param_list = ['BusNum', 'BusName', 'ID', 'AllLabels', 'Status', 'GenMW', 'GenMWMax']
load_param_list = ['BusNum', 'BusName', 'ID', 'AreaNum', 'Status', 'LoadMW']


def get_bus_branch(filecase, bus_paramter, branch_paramter, bus_filter, branch_filter):
    simauto_obj = win32com.client.Dispatch('pwrworld.SimulatorAuto')
    simauto_obj.OpenCase(filecase)

    bus_df = get_object(simauto_obj, 'Bus', bus_paramter, bus_filter)
    print("bus_df =", bus_df.head())
    branch_df = get_object(simauto_obj, 'Branch', branch_paramter, branch_filter)

    return [bus_df, branch_df]


def get_object(simauto_obj, object_type, param_list, filter_name):
    object_tuple = simauto_obj.GetParametersMultipleElementFlatOutput(object_type, param_list, filter_name)
    object_array = np.array(object_tuple[3:]).reshape(-1, len(param_list))
    object_df = pd.DataFrame(object_array, columns=param_list)
    return object_df


filecase = r"Y:\11_Transmission Analysis\03 - Power Flow Cases\WECC\2023 Series\23HS4a1PW\23HS4a1.PWB"
busBranchDF = get_bus_branch(filecase, bus_param_list, branch_param_list, '', '')
# print("busBranchDF =", busBranchDF)
