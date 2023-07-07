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
import numpy as np
import time
from collections import deque

os.chdir("//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Network Topology")

base_directory = os.getcwd()
sys.path.append("/Python Scripts")

from Graph import Network
from Node import Node
from Edge import Edge
from utils import levenshtein, optimal_matching
from typing import *

# Global parameters & variables
start_time = time.time()
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

def search_depth(graph: Network, start_node: str, depth: int) -> Tuple[Set[Node], Set[Edge]]:
    """
    Helper method that performs a Breadth-first search on an input Network graph from
    a starting node in order to accumulate the set of nodes and edges within
    a certain neighborhood from the starting node.

    Inputs:
        - graph: An input Network
        - start_node: The name of the to start from, i.e. "MIDWAY10"
        - depth: The maximum depth to search. Should be at least 1.
    
    Outputs:
        - nodes: The set of all visited Nodes within the depth
        - edges: The set of all traversed Edges within the depth
    """
    start_node = graph.get_node(start_node)
    queue = deque([(start_node, depth)])
    nodes = []
    edges = []

    while queue:
        node, curr_depth = queue.popleft()
        nodes.append(node)

        if curr_depth > 0:
            for edge in graph.get_neighbors(node.name):
                if edge not in edges:
                    edges.append(edge)
                    neighbor = edge.node1 if edge.node1 != node else edge.node2
                    queue.append((neighbor, curr_depth - 1))
    
    nodes.remove(start_node)

    # Display the Nodes and Edges for Debugging purposes
    for node in nodes:
        print(node)
    
    print("======================================")
    for edge in edges:
        print(edge)

    return nodes, edges

def compare_sets(list1: List, list2: List) -> float:
    """
    Compares the proximity of two sets of elements utilizing the optimal matching
    algorithm in utils. 
    
    Assume list1 and list2 are of the same time (either both Nodes or both Edges)
    """
    similarity_matrix = np.zeros((len(list1), len(list2)))

    for i in range(0, len(list1)):
        for j in range(0, len(list2)):
            similarity_matrix[i][j] = list1[i].simple_compare(list2[j])
    
    return optimal_matching(list1, list2, similarity_matrix)



CRR_Network = build_graph(CRR_sheet)
WECC_Network = build_graph(WECC_sheet)

crr_nodes, crr_edges = search_depth(CRR_Network, "MIDWAY10", 4)
# crr_nodes.remove(Node("ZP26SL 1", 90003))
print("=======================================================")
wecc_nodes, wecc_edges = search_depth(WECC_Network, "MIDWAY", 4)

print(compare_sets(crr_nodes, wecc_nodes))

# Output Summary Statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")