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
from anytree import Node, RenderTree
from anytree.exporter import UniqueDotExporter
from collections import deque

os.chdir("//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Network Topology")

base_directory = os.getcwd()
sys.path.append("/Python Scripts")

from Graph import Network
from Node import Bus
from Edge import Edge
from utils import levenshtein, optimal_matching, name_compare, find_node
from typing import *

# Global parameters & variables
start_time = time.time()
CRR_sheet = "./Input Data/CRR Buses and Branches.xlsx"
WECC_sheet = "./Input Data/WECC Buses and Branches.xlsx"


def extract_nodes(sheet_path: str, from_name: str, to_name: str, zone: List[str]) -> List[str]:
    """
    Basic helper method that extracts all buses from an Excel branch
    data sheet that are within a certain zone.

    Inputs:
        - sheet_path: Path to the Excel sheet.
        - zone: The list of names of the desired zones/areas
    
    Output:
        - The list of Node names within that zone.
    """
    df_branch = pd.read_excel(sheet_path, sheet_name="Branch")
    filtered_df = df_branch.loc[(df_branch[from_name].isin(zone)) & (df_branch[to_name].isin(zone))]
    nodes =  filtered_df[['From Name', 'To Name']].values.flatten().tolist()

    return list(set(nodes))


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
        row_node = Bus.from_row(row)
        base_graph.add_node(row_node)

    for _, row in df_branch.iterrows():
        row_edge = Edge.from_row(row)
        base_graph.add_edge(row_edge)

    return base_graph


def search_depth(
    graph: Network, start_node: str, depth: int
) -> Tuple[Set[Bus], Set[Edge]]:
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
    return nodes, edges


def compare_sets(list1: List, list2: List, verbose=False) -> float:
    """
    Compares the proximity of two sets of elements utilizing the optimal matching
    algorithm in utils.

    Assume list1 and list2 are of the same time (either both Nodes or both Edges)
    """
    similarity_matrix = np.zeros((len(list1), len(list2)))

    for i in range(0, len(list1)):
        for j in range(0, len(list2)):
            comparison = list1[i].simple_compare(list2[j])

            if isinstance(list1[i], Bus):
                if comparison[1] < 0.3:
                    node1_neighbors = len(search_depth(CRR_Network, list1[i].name, 1)[0])
                    node2_neighbors = len(search_depth(WECC_Network, list2[j].name, 1)[0])
                    similarity_matrix[i][j] += min(node1_neighbors, node2_neighbors) / max(node1_neighbors, node2_neighbors) * 0.65
                    similarity_matrix[i][j] += comparison[0] * 0.35
                
                else:
                    similarity_matrix[i][j] = sum(comparison) / len(comparison)
            
            else:
                similarity_matrix[i][j] = sum(comparison) / len(comparison)

    return optimal_matching(list1, list2, similarity_matrix, verbose=verbose)[0]


def topology_comp(net1: Network, node1: str, net2: Network, node2: str, depth: int, n_w=0.4, e_w=0.6, verbose=False) -> float:
    """
    Function that compares the topologies between two nodes in two distinct networks
    by computing a weighted average between the similarity of the nodes
    and edges a certain depth away from each input node.

    Inputs:
        - net1, net2: The two input networks.
        - node1, node2: The corresponding node names from the two input networks.
        - depth: How deep to search in each graph.
        - n_w, e_w: The weights of the node and edge similarities, 
                    (0.4, 0.6) by default.
        - verbose: Boolean flag that determines if neighborhood nodes/edges should
                   be displayed.

    Output:
        - A float in [0, 1] giving the similarity of the topologies.
    """
    net1_nodes, net1_edges = search_depth(net1, node1, depth)
    net2_nodes, net2_edges = search_depth(net2, node2, depth)

    if len(net1_nodes) == 0:
        return 1 / len(net2_nodes) if len(net2_nodes) > 0 else 1
    
    if len(net2_nodes) == 0:
        return 1 / len(net1_nodes) if len(net2_nodes) > 0 else 1
    
    return compare_sets(net1_nodes, net2_nodes, verbose) * n_w + compare_sets(net1_edges, net2_edges, verbose) * e_w


def similiarity(net1: Network, node1: str, net2: Network, node2: str, depth: int, weights = [0.35, 0.25, 0.4], verbose=False) -> float:
    """
    Overall function that evaluates the similarity between any two nodes
    from two different networks. Computes a weighted average between
    the loseness of the nodes' names, bus IDs, and topologies. 

    Inputs:
        - net1, net2: The two input networks.
        - node1, node2: The corresponding node names from the two input networks.
        - depth: How deep to search in each graph.
    
    Output:
        - The similiarity score from [0, 1].
    """
    name_score = name_compare(node1, node2)
    n_1 = net1.get_node(node1)
    n_2 = net2.get_node(node2)

    if n_1.number == n_2.number:
        number_score = 1
        
    else:
        self_num = str(n_1.number)
        other_num = str(n_2.number)

        if self_num[0] == other_num[0]:
            penalty = 0.8
        
        else:
            penalty = 0.5
        
        number_score = max(levenshtein(self_num, other_num), levenshtein(other_num, self_num)) * penalty

        if number_score < 0.3:
            weights = [0.3, 0, 0.7]
    
    if verbose:
        print("Name Score: " + str(name_score))
        print("Number Score: " + str(number_score))

    top_score = topology_comp(net1, node1, net2, node2, depth, verbose=verbose)

    if verbose:
        print("Topology Score: " + str(top_score))

    scores = [name_score, number_score, top_score]
    return sum(a * b for a, b in zip(scores, weights))


def map_populate(net_1: Network, node_1: str, net_2: Network, node_2: Bus, curr: Dict[str, str], tree_nodes: List[Node], threshold=0.7, limit=50):
    """
    A recursive algorithm to populate a set of mappings by starting from a ground truth of matching
    nodes. Recurses in a 'breadth-first' manner - begins by attempting to match the input nodes'
    first layer of neighbors and recurses on similar neighbors.

    Terminates when the length of the mapping exceeds the input limit.

    Inputs:
        - net_1, node_1, net_2, node_2: The corresponding nodes from each network. node_1 and node_2
          are assumed to be strong matches.
        - curr: The current mapping of matches.
        - threshold: Only accept nodes with a similarity above this threshold. 0.85 by default.
        - limit: Terminate when the length of mapping exceeds this. 10 by default.
    
    Output:
        Returns nothing, but populates the input curr mapping.
    """
    if len(curr) >= limit:
        return
    
    for neighbor_1 in search_depth(net_1, node_1, 1)[0]:
        for neighbor_2 in search_depth(net_2, node_2, 1)[0]:
            if neighbor_1.name not in curr and neighbor_2.name not in curr.values():
                if similiarity(net_1, neighbor_1.name, net_2, neighbor_2.name, 2) > threshold:
                    # Add this new match the the current mapping
                    curr[neighbor_1.name] = neighbor_2.name

                    # Add in the node!
                    neighbor_node = Node(neighbor_1.name + " " + neighbor_2.name, parent=tree_nodes[find_node(tree_nodes, node_1 + " " + node_2)])
                    tree_nodes.append(neighbor_node)

                    # Recurse on the neighbors.
                    map_populate(net_1, neighbor_1.name, net_2, neighbor_2.name, curr, tree_nodes)



CRR_Network = build_graph(CRR_sheet)
WECC_Network = build_graph(WECC_sheet)

gt_1 = "MIDWAY10"
gt_2 = "MIDWAY"

curr_mapping = {gt_1: gt_2}
tree_nodes = [Node(gt_1 + " " + gt_2)]

map_populate(CRR_Network, gt_1, WECC_Network, gt_2, curr_mapping, tree_nodes)

for key, val in curr_mapping.items():
    print(key + " " + val)

UniqueDotExporter(tree_nodes[0]).to_picture("recursive_bus_traversal.png")

"""
CRR_Network.remove_edge("MIDWAY10", "ZP26SL 1")

pge_crr = extract_nodes(CRR_sheet, "From Zone Name", "To Zone Name", ["PGAE-30", "SCE-24"])
pge_wecc = extract_nodes(WECC_sheet, "From Area Name", "To Area Name", ["PG AND E", 'SOCALIF'])

# print(similiarity(CRR_Network, "LUGO   6", WECC_Network, "LUGO", depth=1, verbose=True))

output_df = pd.DataFrame()
crr_nodes = []
wecc_nodes = []
similarities = []

for crr in pge_crr:
    for wecc in pge_wecc:   
        crr_nodes.append(crr)
        wecc_nodes.append(wecc)
        similarities.append(similiarity(CRR_Network, crr, WECC_Network, wecc, depth=2))

output_df['CRR Buses'] = crr_nodes
output_df['WECC Buses'] = wecc_nodes
output_df['Similarity'] = similarities

np_sims = np.reshape(similarities, (len(pge_crr), len(pge_wecc)))
optimal_matching(pge_crr, pge_wecc, np_sims, verbose=True)

output_df.to_csv("./Input Data/Similiarity Table.csv", index=False)

"""

# Output Summary Statistics
end_time = time.time()
execution_time = end_time - start_time

print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
