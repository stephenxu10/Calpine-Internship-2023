# type: ignore

import os
import sys
"""
Basic Representation of an Edge in a Network Topology Graph.

Stores the following attributes:
    - FromNode, ToNode
    - DeviceType
    - Circuit Number
    - R, X, B, LimA, LimB, LimC
"""
"""
os.chdir("//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Network Topology")

base_directory = os.getcwd()
sys.path.append("/Python Scripts")
"""

from Node import Node
from utils import num_proximity

class Edge:
    def __init__(self, node1: Node, node2: Node, device_type, circuit_number, r, x, b, lim_a, lim_b, lim_c):
        self.node1 = node1
        self.node2 = node2
        self.device_type = device_type
        self.circuit_number = circuit_number
        self.r = r
        self.x = x
        self.b = b
        self.lim_a = lim_a
        self.lim_b = lim_b
        self.lim_c = lim_c
    
    @classmethod
    def from_row(self, row):
        return Edge(
            Node(row["From Name"], row["From Number"]),
            Node(row["To Name"], row["To Number"]),
            row["Branch Device Type"],
            row["Circuit"],
            row["R"],
            row["X"],
            row["B"],
            row["Lim MVA A"],
            row["Lim MVA B"],
            row["Lim MVA C"],
        )


    def __str__(self):
        return f"Edge({self.node1} <--> {self.node2}): {self.device_type}, Circuit {self.circuit_number}"

    def get_attributes(self):
        return self.device_type, self.circuit_number, self.r, self.x, self.b, self.lim_a, self.lim_b, self.lim_c
    
    def simple_compare(self, other, ratios = [0.125] * 8):
        # Circuit Number comparison
        circuit = 1 if self.circuit_number == other.circuit_number else 0.5

        # Device Type comparison
        device = 1 if self.device_type == other.device_type else 0.5

        # R, X, B comparisons
        r, x, b = num_proximity(self.r, other.r), num_proximity(self.x, other.x), num_proximity(self.b, other.b)

        if self.lim_a == "99999" or other.lim_a == "99999":
            ratios = [0.2] * 5
            comps = [circuit, device, r, x, b]

        else:
            lim_a, lim_b, lim_c = num_proximity(self.lim_a, other.lim_a), num_proximity(self.lim_b, other.lim_b), num_proximity(self.lim_b, other.lim_c)
            comps = [circuit, device, r, x, b, lim_a, lim_b, lim_c]
        return sum(a * b for a, b in zip(comps, ratios))