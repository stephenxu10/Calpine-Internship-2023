"""
Some utility methods for the project
"""
import numpy as np
from scipy.optimize import linear_sum_assignment
from typing import List, Dict, Tuple

def num_proximity(num1: float, num2: float):
    """
    Provides a basic metric to compare the closeness of two numbers.
    """
    if num1 == 0 and num2 == 0:
        return 1
    elif num1 == 0 or num2 == 0:
        return 0.5
    else:
        return min(num1, num2) / max(num1, num2)

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



def optimal_matching(set1: List, set2: List, similarity_scores) -> Tuple[float, List[int], List[int]]:
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

    for row, col in zip(row_indices, col_indices):
        if row < len(set1) and col < len(set2):
            similarity_score = similarity_matrix[row, col]
            print(f"1: {set1[row]}, 2: {set2[col]}, Similarity Score: {similarity_score}")

    # Adjust the similarity score for size differences
    overall_similarity *= (min(len(set1), len(set2)) / max_size)

    print(f"Overall Similarity Score: {overall_similarity}")
    return overall_similarity, row_indices, col_indices

if __name__ == "__main__":
    # Example sets and similarity scores
    set1 = ['A', 'B', 'C', 'D', 'E', 'F']
    set2 = ['U', 'V', 'W']
    similarity_scores = np.random.rand(len(set1), len(set2))

    print(similarity_scores)
    overall_sim, row_idxs, col_idxs = optimal_matching(set1, set2, similarity_scores)
