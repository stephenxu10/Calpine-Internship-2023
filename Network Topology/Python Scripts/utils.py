"""
Some utility methods for the project
"""
import numpy as np
from scipy.optimize import linear_sum_assignment
from anytree import Node
from typing import List, Dict, Tuple, Union, Set
import re

def num_proximity(num1: float, num2: float) -> float:
    """
    Provides a basic metric to compare the closeness of two numbers.
    """
    if num1 == 0 and num2 == 0:
        return 1
    elif num1 == 0 or num2 == 0:
        return 0.5
    else:
        abs_diff = abs(num1 - num2)
        max_diff = max(abs(num1), abs(num2))
        similarity = 1 - (abs_diff / max_diff)
        return similarity
    
def find_node(nodes: List[Node], target: str) -> Union[int, None]:
    """
    Simple helper function to find the index in a list of nodes
    whose name matches the target node.
    """
    for i in range(len(nodes)):
        raw_name = nodes[i].name.split("\n")[0]
        if raw_name == target:
            return i
    
    return None

def find_other_neighbor(neighbors: Set, curr: Dict, visited):
    if(neighbors[0].name in curr or neighbors[0].name in curr.values()):
        return neighbors[1]
    
    if (neighbors[0].name in visited):
        return neighbors[1]
    else:
        return neighbors[0]

def set_node_color(node: Node):
    """
    Node coloring method for Graphviz.
    """
    score = node.name.split("\n")[1].strip()

    if score == "":
        return "color=blue"

    sim_score = float(score)

    if sim_score > 0.65:
        return "color=red"

    else:
        return "color=black"

def levenshtein(x: str, y: str, c_i: float = 1.0, c_d: float = 1.0, c_s: float = 1.0) -> float:
    """
    Helper method that computes the Levenshtein Ratio from one string to another. The Levenshtein Ratio
    is defined as the ratio between the edit distance and the largest string length.

    Finishes in O(|x||y|) time.

    Inputs:
        - x, y: The two input strings.
        - c_i: The cost for insertion. 1 by default.
        - c_d: The cost for deletion. 1 by default.
        - c_s: The cost for substitution. 1 by default
    """
    x = x.lower()
    y = y.lower()

    m = len(x)
    n = len(y)

    dp = [[0] * (n + 1) for _ in range(m + 1)]

    for j in range(0, n + 1):
        dp[0][j] = j * c_i

    for i in range(0, m + 1):
        dp[i][0] = i * c_d

    for i in range(1, m + 1):
        for j in range(1, n + 1):
            insert = dp[i][j-1] + c_i
            delete = dp[i-1][j] + c_d
            sub = dp[i - 1][j - 1] if x[i-1] == y[j-1] else dp[i - 1][j - 1] + c_s

            dp[i][j] = min(insert, min(delete, sub))

    return 1 - dp[m][n] / max(m, n)


def isSubsequence(substring: str, string: str) -> bool:
    """
    Simple recursive algorithm to determine if one string is a subsequence of the other.
    For example,
        "foo" is a subsequence of "goodfood"
        "test" is not a subsequence of "ttes12345"
    """
    if len(substring) > len(string):
        return False

    if len(substring) == 0:
        return True

    if substring[-1] == string[-1]:
        return isSubsequence(substring[:-1], string[:-1])
    
    else:
        return isSubsequence(substring, string[:-1])

def name_compare(x: str, y: str) -> float:
    """
    Helper method to compare two input Bus Names. Returns a float in [0, 1] that represents
    their similiarity.
    
    Strips numbers from each string and compares them through their Edit Distance. Also
    accounts for a special case when a string is in the format XX_YY_##
    """
    y_regex = r"[\w]+_[\w]+_[\d]*"

    x = re.sub(r'\d+', '', x)
    y = re.sub(r'\d+', '', y)
    x = re.sub(r'[aeiouAEIOU]', '', x)
    y = re.sub(r'[aeiouAEIOU]', '', y)

    x = x.replace(' ', '')
    y = y.replace(' ', '')

    if isSubsequence(x, y) or isSubsequence(y, x):
        return 1.0
    
    if "_" in x:
        x_components = x.split("_")[:2]

        for loc in x_components:
            if isSubsequence(loc, y):
                return 1
        
        return 0.3
    
    elif re.match(y_regex, y):
        y_components = y.split("_")[:2]

        if y_components[0][0] != x[0] and y_components[1][0] != x[0]:
            return 0.3

        for loc in y_components:
            if isSubsequence(loc, x):
                return 1
        
        return 0.3
    
    # Otherwise, neither string has any underscores - use the edit distance
    return max(levenshtein(x, y), levenshtein(y, x))


def optimal_matching(set1: List, set2: List, similarity_scores, verbose=False) -> Tuple[float, List[int], List[int]]:
    """
    A Helper method that implements the Hungarian Algorithm for the Assignment Problem. 
    Given two sets of elements and a similarity score mapping, this algorithm returns the optimal Bipartite matching 
    between the sets that maximizes the sum of their similiarity scores.

    The procedure is slightly modified for our purposes. We enforce a matching penalty
    if the two sets have different size, as shown in the code.
    
    Inputs:
        - set1: A List of elements in the first set
        - set2: A List of elements in the second set
        - similarity_scores: A mapping that yields the similarity score between every pair of elements.
        - verbose: Flag that determines if the output matching should be printed
    
    Output:
    Returns a tuple of three elements
        - overall_similarity: The average similarity from the optimal matching
        - row_indicies, col_indices: List of Ints that give the coordinates of the specific matching
    """

    # Determine the maximum size of the sets
    max_size = max(len(set1), len(set2))
    min_size = min(len(set1), len(set2))

    # Create a square matrix with dummy nodes and similarity scores
    similarity_matrix = np.zeros((max_size, max_size))
    similarity_matrix[:len(set1), :len(set2)] = similarity_scores

    # Apply the Hungarian algorithm
    row_indices, col_indices = linear_sum_assignment(-similarity_matrix)
    overall_similarity = similarity_matrix[row_indices, col_indices].sum() / min_size

    if verbose:
        for row, col in zip(row_indices, col_indices):
            if row < len(set1) and col < len(set2):
                similarity_score = similarity_matrix[row, col]
                if similarity_score > 0.6:
                    print(f"1: {set1[row]}, 2: {set2[col]}, Similarity Score: {similarity_score}")

    # Adjust the similarity score for size differences
    overall_similarity *= (min(len(set1), len(set2)) / max_size)

    # print(f"Overall Similarity Score: {overall_similarity}")
    return overall_similarity, row_indices, col_indices

if __name__ == "__main__":
    # Example sets and similarity scores
    set1 = ['A', 'B', 'C', 'D', 'E', 'F']
    set2 = ['U', 'V', 'W']
    similarity_scores = np.random.rand(len(set1), len(set2))

    print(similarity_scores)
    overall_sim, row_idxs, col_idxs = optimal_matching(set1, set2, similarity_scores)
