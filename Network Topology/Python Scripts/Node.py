# type: ignore

import os
import sys
"""
Basic representation of a Bus (Node) in our Network Graph.

Stores the following attributes:
    1) Name
    2) Number
"""

os.chdir("//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Network Topology")

base_directory = os.getcwd()
sys.path.append("/Python Scripts")


from utils import levenshtein, name_compare

class Node:
    def __init__(self, name, num):
        self.name = name
        self.number = num
    
    @classmethod
    def from_row(self, row):
        return Node(row['Name'], row['Number'])

    def __str__(self):
        return f"Node({self.name}, {self.number})"
    
    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, Node):
            return self.name == other.name and self.number == other.number
        return False
    
    def simple_compare(self, other, name_weight=0.6, num_weight=0.4) -> float:
        assert(name_weight + num_weight == 1)

        name_ratio = name_compare(self.name, other.name)

        if self.number == other.number:
            number_ratio = 1
        
        else:
            self_num = str(self.number)
            other_num = str(other.number)

            if self_num[0] == other_num[0]:
                penalty = 0.8
            
            else:
                penalty = 0.5
            
            number_ratio = max(levenshtein(self_num, other_num), levenshtein(other_num, self_num)) * penalty
        
        return name_ratio * name_weight + number_ratio * num_weight