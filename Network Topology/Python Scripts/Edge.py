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

os.chdir("//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Network Topology")

base_directory = os.getcwd()
sys.path.append("/Python Scripts")

from Node import Node

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