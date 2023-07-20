# type: ignore

import os
import sys
"""
Basic representation of the Network Topology as an undirected graph. The basic form
is an adjacency list - each node is mapped to its immediate neighbors.
"""

os.chdir("//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Network Topology/Python Scripts")

from Node import Bus
from Edge import Edge
from typing import List

class Network:
    def __init__(self):
        self.graph = {}
    
    def __str__(self):
        for node in self.graph:
            for edge in self.graph[node]:
                print(str(edge.node1) + " -> " + str(edge.node2) + " " + str(edge.get_attributes()))

    
    def add_node(self, node: Bus):
        if node not in self.graph:
            self.graph[node] = []
    
    def add_edge(self, edge: Edge):
        from_node = edge.node1
        to_node = edge.node2

        if from_node in self.graph and to_node in self.graph:
            self.graph[from_node].append(edge)
            self.graph[to_node].append(edge)
    
    def get_node(self, name: str) -> Bus:
        for node in self.graph:
            if node.name == name:
                return node
        return ""
    
    def get_neighbors(self, name: str) -> List[Edge]:
        for node in self.graph:
            if node.name == name:
                return self.graph[node]
        
        return ""
    
    def remove_edge(self, from_name: str, neighbor_name: str):
        for node in self.graph:
            if node.name == from_name:
                for edge in self.graph[node]:
                    if edge.node1.name == neighbor_name or edge.node2.name == neighbor_name:
                        self.graph[node].remove(edge)
        
    def display_neighbors(self, name: str) -> None:
        neighbors = self.get_neighbors(name)

        if neighbors != "":
            for edge in neighbors:
                print(str(edge))
    

    