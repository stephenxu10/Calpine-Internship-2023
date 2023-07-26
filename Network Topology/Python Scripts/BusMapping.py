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
import pandas as pd
import numpy as np
import time
from anytree import Node
from anytree.exporter import DotExporter
from collections import deque

os.chdir("//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Network Topology/Python Scripts")

from Graph import Network
from Node import Bus
from Edge import Edge
from utils import levenshtein, optimal_matching, name_compare, find_node, set_node_color, find_other_neighbor, descending_score_idxs, find_non_parent
from typing import *

# Global parameters & variables
start_time = time.time()
CRR_sheet = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Network Topology/Input Data/CRR Buses and Branches.xlsx"
WECC_sheet = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Network Topology/Input Data/WECC Buses and Branches.xlsx"

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
    nodes =  df_branch[['From Name', 'To Name']].values.flatten().tolist()

    return list(set(nodes))


def build_graph(sheet_path: str, version: int) -> Network:
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
        if version == 1:
            #if row['From Zone Name'] in ["BPA-43"] or row['To Zone Name'] in ["BPA-43"]:
            row_edge = Edge.from_row(row)
            base_graph.add_edge(row_edge)
        
        elif version == 2:
           #if row['From Area Name'] in ["NORTHWEST"] or row['To Area Name'] in ["NORTHWEST"]:
            row_edge = Edge.from_row(row)
            base_graph.add_edge(row_edge)

    return base_graph


def search_depth(
    graph: Network, start_node: str, start_number: int, depth: int
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
    start = graph.get_node(start_node, start_number)

    queue = deque([(start, depth)])
    nodes = []
    edges = []

    while queue:
        node, curr_depth = queue.popleft()
        nodes.append(node)

        if curr_depth > 0:
            for edge in graph.get_neighbors(node.name, node.number):
                if edge not in edges:
                    edges.append(edge)
                    neighbor = edge.node1 if edge.node1 != node else edge.node2
                    queue.append((neighbor, curr_depth - 1))

    nodes.remove(start)
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
                    node1_neighbors = len(search_depth(CRR_Network, list1[i].name, list1[i].number, 1)[0])
                    node2_neighbors = len(search_depth(WECC_Network, list2[j].name, list2[j].number, 1)[0])
                    similarity_matrix[i][j] += min(node1_neighbors, node2_neighbors) / max(node1_neighbors, node2_neighbors) * 0.65
                    similarity_matrix[i][j] += comparison[0] * 0.35
                
                else:
                    similarity_matrix[i][j] = sum(comparison) / len(comparison)
            
            else:
                similarity_matrix[i][j] = sum(comparison) / len(comparison)

    return optimal_matching(list1, list2, similarity_matrix, verbose=verbose)[0]


def topology_comp(net1: Network, node1: str, num1: int, net2: Network, node2: str, num2: int, depth: int, n_w=0.4, e_w=0.6, verbose=False) -> float:
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
    net1_nodes, net1_edges = search_depth(net1, node1, num1, depth)
    net2_nodes, net2_edges = search_depth(net2, node2, num2, depth)

    if len(net1_nodes) == 0:
        return 1 / len(net2_nodes) if len(net2_nodes) > 0 else 1
    
    if len(net2_nodes) == 0:
        return 1 / len(net1_nodes) if len(net2_nodes) > 0 else 1
    
    return compare_sets(net1_nodes, net2_nodes, verbose) * n_w + compare_sets(net1_edges, net2_edges, verbose) * e_w


def similiarity(net1: Network, node1: str, num1: int, net2: Network, node2: str, num2: int, depth: int, weights = [0.35, 0.25, 0.4], verbose=False) -> float:
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
    n_1 = net1.get_node(node1, num1)
    n_2 = net2.get_node(node2, num2)

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
            weights = [0.35, 0, 0.65]
    
    if verbose:
        print("Name Score: " + str(name_score))
        print("Number Score: " + str(number_score))

    top_score = topology_comp(net1, node1, num1, net2, node2, num2, depth, verbose=verbose)

    if verbose:
        print("Topology Score: " + str(top_score))

    scores = [name_score, number_score, top_score]
    return sum(a * b for a, b in zip(scores, weights))


def smart_similarity(net_1: Network, node_1: str, num_1: int, parent_1: str, par_num_1: int, net_2: Network, node_2: str, num_2: str, parent_2: str, par_num_2: int, depth: int) -> float:
    """
    A "smarter" similarity metric intended for use when traversing through map populate. Unlike the original similarity function, 
    this method accounts for scenarios when a tap bus is being compared to a 'hub' bus. The high-level logic is as follows:
        - If the input buses are both hubs, compare them as usual through the similarity function above.
        - If one bus is a hub while the other is a tap, continue iterating through the tap bus until a hub is reached. Compare
          those two once the hub is reached.
        - If both buses are tap buses, compare their corresponding neighbors.

    Inputs:
        - net_1, net_2: The two input networks
        - node_1, num_1, parent_1, par_num_1: The input node in the first network and its parent
        - node_2, num_2, parent_2, par_num_2: The input node in the second network and its parent
        - depth: The depth to compare for similarity
    
    Output:
        - A similarity score from 0 to 1 that gives the proximity between the two input nodes.
    """
    neighbors_1 = list(search_depth(net_1, node_1, num_1, 1)[0])
    neighbors_2 = list(search_depth(net_2, node_2, num_2, 1)[0])

    # Base Case - Both nodes are hub buses
    if len(neighbors_1) != 2 and len(neighbors_2) != 2:
        res = similiarity(net_1, node_1, num_1, net_2, node_2, num_2, depth)
        return res
        
    # Case 2 - The first node is a tap, the second is a hub
    elif len(neighbors_1) == 2 and len(neighbors_2) != 2:
        new_name_1, new_number_1 = find_non_parent(neighbors_1, parent_1, par_num_1)
        return smart_similarity(net_1, new_name_1, new_number_1, node_1, num_1, net_2, node_2, num_2, parent_2, par_num_2, depth)
    
    # Case 3 - The first node is a tap, the second is a hub
    elif len(neighbors_1) != 2 and len(neighbors_2) == 2:
        new_name_2, new_number_2 = find_non_parent(neighbors_2, parent_2, par_num_2)
        return smart_similarity(net_1, node_1, num_1, parent_1, par_num_1, net_2, new_name_2, new_number_2, node_2, num_2, depth)

    # Case 4 - Both nodes are Tap Buses
    else:
        new_name_1, new_number_1 = find_non_parent(neighbors_1, parent_1, par_num_1)
        new_name_2, new_number_2 = find_non_parent(neighbors_2, parent_2, par_num_2)
        return smart_similarity(net_1, new_name_1, new_number_1, node_1, num_2, net_2, new_name_2, new_number_2, node_2, num_2, depth)


def map_populate(net_1: Network, node_1: str, num1: int, net_2: Network, node_2: Bus, num2: int, curr: Dict[str, str], tree_nodes: List[Node], visited: Set[str], sim: float, threshold=0.5):
    """
    A recursive algorithm to populate a set of mappings by starting from a ground truth of matching
    nodes. Recurses in a 'depth-first' manner - begins by attempting to match the input nodes'
    first layer of neighbors and recurses on their similar neighbors.

    Terminates when the length of the mapping exceeds the input limit.

    Inputs:
        - net_1, node_1, num1, net_2, node_2, num2: The corresponding node & numbers from each network. node_1 and node_2
          are assumed to be strong matches.
        - curr: The current mapping of matches.
        - visited: The current visited set of Tap Buses
        - threshold: Only accept nodes with a similarity above this threshold. 0.5 by default.
        - sim: The similarity score between node_1 and node_2. Infinity if either node is a tap bus.

    Output:
        Returns nothing, but populates the input curr mapping.
    """
    print(node_1 + " " + node_2 + " " + str(sim)) 
    # Grab the immediate neighbors of each input node.
    neighbors_1 = list(search_depth(net_1, node_1, num1, 1)[0])
    neighbors_2 = list(search_depth(net_2, node_2, num2, 1)[0])
    
    # If both nodes are tap buses, recuse on the unvisited neighbors.
    if len(neighbors_1) == 2 and len(neighbors_2) == 2:
        other_1 = find_other_neighbor(neighbors_1, curr, visited)
        other_2 = find_other_neighbor(neighbors_2, curr, visited)

        # If we have already examined the other neighbors, there is no reason to revisit them.
        if other_1.name in visited and other_2.name in visited or other_1.name in curr or other_2.name in curr.values():
            return

        # If the new pair of neighbors are both non-tap buses, calculate their similarity and recuse on them.
        if len(net_1.get_neighbors(other_1.name, other_1.number)) != 2 and len(net_2.get_neighbors(other_2.name, other_2.number)) != 2:
            sim_score = similiarity(net_1, other_1.name, other_1.number, net_2, other_2.name, other_2.number, 2)
            neighbor_node = Node(other_1.name + " " + other_2.name + "\n " + str(round(sim_score, 3)), parent=tree_nodes[find_node(tree_nodes, node_1 + " " + node_2)])
            tree_nodes.append(neighbor_node)
            map_populate(net_1, other_1.name, other_1.number, net_2, other_2.name, other_2.number, curr, tree_nodes, visited, sim_score)

        # Otherwise, recurse on the neighbors and indicate that one is a tap bus.
        else:
            # No similarity in the node name means that one of the nodes is a tap bus.
            neighbor_node = Node(other_1.name + " " + other_2.name + "\n ", parent=tree_nodes[find_node(tree_nodes, node_1 + " " + node_2)])
            tree_nodes.append(neighbor_node)
            visited.add(other_1.name)
            visited.add(other_2.name)
            map_populate(net_1, other_1.name, other_1.number, net_2, other_2.name, other_2.number, curr, tree_nodes, visited, float('inf'))
    
    elif len(neighbors_1) == 2:
        if node_2 in curr.values():
            print(curr)
            return
        
        other_1 = find_other_neighbor(neighbors_1, curr, visited)

        if other_1.name in visited:
            return
        
        if len(net_1.get_neighbors(other_1.name, other_1.number)) != 2:
            sim_score = smart_similarity(net_1, other_1.name, other_1.number, node_1, num1, net_2, node_2, num2, "", "", 2, False)
            neighbor_node = Node(other_1.name + " " + node_2 + "\n " + str(round(sim_score, 3)), parent=tree_nodes[find_node(tree_nodes, node_1 + " " + node_2)])
            tree_nodes.append(neighbor_node)
            map_populate(net_1, other_1.name, other_1.number, net_2, node_2, num2, curr, tree_nodes, visited, sim_score)            

        else:    
            neighbor_node = Node(other_1.name + " " + node_2 + "\n ", parent=tree_nodes[find_node(tree_nodes, node_1 + " " + node_2)])
            tree_nodes.append(neighbor_node)
            visited.add(other_1.name)
            map_populate(net_1, other_1.name, other_1.number, net_2, node_2, num2, curr, tree_nodes, visited, float('inf'))
    
    elif len(neighbors_2) == 2:
        if node_1 in curr:
            return 
        
        other_2 = find_other_neighbor(neighbors_2, curr, visited)

        if other_2.name in visited:
            return
        
        if len(net_2.get_neighbors(other_2.name, other_2.number)) != 2:
            sim_score = smart_similarity(net_1, node_1, num1, "", "", net_2, other_2.name, other_2.number, node_2, num2, 2, False)
            neighbor_node = Node(node_1 + " " + other_2.name + "\n " + str(round(sim_score, 3)), parent=tree_nodes[find_node(tree_nodes, node_1 + " " + node_2)])
            tree_nodes.append(neighbor_node)
            map_populate(net_1, node_1, num1, net_2, other_2.name, other_2.number, curr, tree_nodes, visited, sim_score)            

        else:    
            neighbor_node = Node(node_1 + " " + other_2.name + "\n ", parent=tree_nodes[find_node(tree_nodes, node_1 + " " + node_2)])
            tree_nodes.append(neighbor_node)
            visited.add(other_2.name)
            map_populate(net_1, node_1, num1, net_2, other_2.name, other_2.number, curr, tree_nodes, visited, float('inf'))
    
    # Otherwise, both input nodes are hub buses.
    else:
        # Compute the matrix of similarities between all pairs of neighbors.
        similarities = []
        if sim > threshold:
            curr[node_1] = node_2
            for crr_n in neighbors_1:
                for wecc_n in neighbors_2:   
                    similarities.append(smart_similarity(net_1, crr_n.name, crr_n.number, node_1, num1, net_2, wecc_n.name, wecc_n.number, node_2, num2, 2, False))

            # Find the optimal matching between the CRR neighbors and WECC neighbors.
            np_sims = np.reshape(similarities, (len(neighbors_1), len(neighbors_2)))
            _, row_indices, col_indices = optimal_matching(neighbors_1, neighbors_2, np_sims)

            # Iterate in a greedy manner - explore the most fruitful matches first.
            for idx in descending_score_idxs(row_indices, col_indices, np_sims):
                row = row_indices[idx]
                col = col_indices[idx]
                if row < len(neighbors_1) and col < len(neighbors_2):
                    similarity_score = np_sims[row, col]

                    crr_match = neighbors_1[row]
                    wecc_match = neighbors_2[col]

                    # Only recuse if the matches have not been visited yet.
                    if crr_match.name not in curr and wecc_match.name not in curr.values() and wecc_match.name not in visited and crr_match.name not in visited:
                        if len(search_depth(net_1, crr_match.name, crr_match.number, 1)[0]) != 2 and len(search_depth(net_2, wecc_match.name, wecc_match.number, 1)[0]) != 2:
                            curr[crr_match.name] = wecc_match.name
                            idx = find_node(tree_nodes, node_1 + " " + node_2)
                            neighbor_node = Node(crr_match.name + " " + wecc_match.name + "\n" + str(round(similarity_score, 3)), parent=tree_nodes[idx])
                            tree_nodes.append(neighbor_node)

                            # Recurse on the neighbors.
                            map_populate(net_1, crr_match.name, crr_match.number, net_2, wecc_match.name, wecc_match.number, curr, tree_nodes, visited, similarity_score)

                        else:
                            idx = find_node(tree_nodes, node_1 + " " + node_2)
                            neighbor_node = Node(crr_match.name + " " + wecc_match.name + "\n ", parent=tree_nodes[idx])
                            tree_nodes.append(neighbor_node)

                            if len(net_1.get_neighbors(crr_match.name, crr_match.number)) == 2 and crr_match.name not in visited:
                                visited.add(crr_match.name)
                            
                            if len(net_2.get_neighbors(wecc_match.name, wecc_match.number)) == 2 and wecc_match.name not in visited:
                                visited.add(wecc_match.name)

                            # Recurse on the neighbors.
                            map_populate(net_1, crr_match.name, crr_match.number, net_2, wecc_match.name, wecc_match.number, curr, tree_nodes, visited, float('inf'))

                
CRR_Network = build_graph(CRR_sheet, 1)
WECC_Network = build_graph(WECC_sheet, 2)
CRR_Network.remove_edge("MIDWAY10", "ZP26SL 1")
# WECC_Network.remove_edge("LOSBANOS", "L.BANS M")

gt_1 = "MIDWAY10"
num_1 = 30060
gt_2 = "MIDWAY"
num_2 = 30060
visited = set()

curr_mapping = {gt_1: gt_2}
tree_nodes = [Node(gt_1 + " " + gt_2 + "\n 1.0")]

map_populate(CRR_Network, gt_1, num_1, WECC_Network, gt_2, num_2, curr_mapping, tree_nodes, visited, 1.0)
DotExporter(tree_nodes[0], nodeattrfunc=set_node_color).to_picture("./Traversals/overall_traversal.png")

print(len(curr_mapping))
for node in curr_mapping:
    print(node + " " + curr_mapping[node])

"""c = extract_nodes(CRR_sheet, "From Zone Name", "To Zone Name", ["PGAE-30", "SCE-24"])
pge_crr = []

for node in c:
    if len(CRR_Network.get_neighbors(node)) != 2:
        pge_crr.append(node)

w = extract_nodes(WECC_sheet, "From Area Name", "To Area Name", ["PG AND E", 'SOCALIF'])
pge_wecc = []

for wecc_node in w:
    if len(WECC_Network.get_neighbors(wecc_node)) != 2:
        pge_wecc.append(wecc_node)

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

output_df.to_csv("//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Network Topology/Similarity Table.csv", index=False)"""

# Output Summary Statistics
end_time = time.time()
execution_time = end_time - start_time

print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")

