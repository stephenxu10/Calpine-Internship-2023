"""
Basic representation of a Bus (Node) in our Network Graph.

Stores the following attributes:
    1) Name
    2) Number
"""
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
            return self.name == other.name
        return False