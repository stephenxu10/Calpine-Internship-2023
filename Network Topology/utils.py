"""
Some utility methods for the project
"""


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

    for arr in dp:
        print(arr)

    return dp[m][n] / max(m, n)


print(levenshtein("Diablo10", "Diablo10", 0.5, 1, 1))

