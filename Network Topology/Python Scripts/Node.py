# type: ignore

import os
import sys
"""
Basic representation of a Bus (Node) in our Network Graph.

Stores the following attributes:
    1) Name
    2) Number
"""

os.chdir("//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Network Topology/Python Scripts")

base_directory = os.getcwd()
sys.path.append("/Python Scripts")


from utils import levenshtein, name_compare
from typing import *

class Bus:
    def __init__(self, name, num):
        self.name = name
        self.number = num
    
    @classmethod
    def from_row(self, row):
        return Bus(row['Name'], row['Number'])

    def __str__(self):
        return f"Bus({self.name}, {self.number})"
    
    def __hash__(self):
        return hash((self.name, self.number))

    def __eq__(self, other):
        if isinstance(other, Bus):
            return self.name == other.name and self.number == other.number
        return False
    
    def simple_compare(self, other) -> Tuple[float, float]:
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
        
        return name_ratio, number_ratio