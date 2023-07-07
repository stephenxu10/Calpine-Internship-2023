# type: ignore

"""
This Python Script aims to create a mapping between buses in two different electrical systems. Buses
are compared and scored based on three criteria:
    1) Similiarity of Names
    2) Matching of Bus Number
    3) Similiarity of Network Topology

These three criteria combine to give an approximate similiarity score from 0 to 1. For the end result,
a mapping is generated between the two sets of buses. 

Currently maps 500 Nom KV Buses from WECC to CRR.
"""

import os
import sys
import pandas as pd

os.chdir("//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Network Topology")

base_directory = os.getcwd()
sys.path.append("/Python Scripts")

from Graph import Network
from Node import Node
from Edge import Edge
from utils import levenshtein

CRR_sheet = "./Input Data/CRR Buses and Branches.xlsx"
WECC_sheet = "./Input Data/WECC Buses and Branches.xlsx"

def build_graph(sheet_path: str) -> Network:
    """
    Helper method that builds a Network given the mapping data from an Excel sheet path.

    Input:
        - sheet_path: A relative path to an Excel Sheet that gives Bus and Branch data
    
    Output:
        - A Network object storing all of the data.
    """
    base_graph = Network()
    df_branch = pd.read_excel(sheet_path, sheet_name="Branch")
    df_bus = pd.read_excel(sheet_path, sheet_name="Bus")

    for _, row in df_bus.iterrows():
        row_node = Node.from_row(row)
        base_graph.add_node(row_node)

    for _, row in df_branch.iterrows():
        row_edge = Edge.from_row(row)
        base_graph.add_edge(row_edge)

    return base_graph


CRR_Network = build_graph(CRR_sheet)
WECC_Network = build_graph(WECC_sheet)

CRR_Network.display_neighbors("MIDWAY10")