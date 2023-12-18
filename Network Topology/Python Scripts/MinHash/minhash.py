from sklearn.utils import murmurhash3_32
from typing import List
import random

def hashfunc(seed: int):
    """
    Simple Hash Function factory that takes in a desired range and returns a hash function that
    maps integers into a table of that length.
    """
    def hash(key):
        return murmurhash3_32(key, seed)
    return hash

def hash_func_to_bucket(k: int, R: int):
    coefficients = [random.randint(0, R - 1) for _ in range(k)]
    constant = random.randint(0, R - 1)
    
    # Define the hash function using the generated coefficients and constant
    def hash_function(values: List[int]):
        hash_value = sum(coefficient * value for coefficient, value in zip(coefficients, values))
        hash_value += constant
        return hash_value % R

    return hash_function

def get_grams(text, k=3):
    return set(text[i:i+k] for i in range(len(text) - k + 1))

def jaccard_similarity(set1, set2):
    return len(set1.intersection(set2)) / len(set1.union(set2))

def MinHashFactory(seed: int):
    def MinHash(A: str, m: int):
        hash_functions = [hashfunc(i + seed) for i in range(m)]
        grams = get_grams(A)
        min_hashes = []
        for i in range(m):
            min_hashes.append(min(hash_functions[i](gram) for gram in grams))
        return min_hashes

    return MinHash

class HashTable:
    def __init__(self, K, L, B, R):
        """
        Initializes a new Hash Table with the following parameters:
        - K: The number of hash functions
        - L: The number of hash tables
        - B: The maximum size of each bucket in the hash table
        - R: The number of buckets in the hash table
        """
        self.K = K
        self.L = L
        self.B = B
        self.R = R
        self.bucket_mapper = hash_func_to_bucket(self.K, self.R)
        self.hashes = [MinHashFactory(l * 1000) for l in range(L)]
        self.tables = [[[] for _ in range(R)] for _ in range(L)]

    def insert(self, hashcodes: List[List[int]], id):
        for i in range(self.L):
            bucket = self.bucket_mapper(hashcodes[i])
            if len(self.tables[i][bucket]) < self.B:
                self.tables[i][bucket].append(id)

    def lookup(self, hashcodes: List[List[int]]):
        items = []
        for i in range(self.L):
            bucket = self.bucket_mapper(hashcodes[i])
            items.extend(self.tables[i][bucket])
        return set(items)

if __name__ == "__main__":
    S1 = ("The mission statement of the WCSCC and area employers recognize the"
          " importance of good attendance on the job. Any student whose absences exceed 18 days is"
          " jeopardizing their opportunity for advanced placement as well as hindering his/her"
          " likelihood for successfully completing their program.")
    S2 = ("The WCSCC’s mission statement and surrounding employers recognize the"
          " importance of great attendance. Any student who is absent more than 18 days will lose the"
          " opportunity for successfully completing their trade program.")

    m = 100
    MinHash = MinHashFactory(1000)
    minhashes_S1 = MinHash(S1, m)
    minhashes_S2 = MinHash(S2, m)
    minhash_matches = sum(1 for i in range(m) if minhashes_S1[i] == minhashes_S2[i])
    estimated_similarity = minhash_matches / m
    set_S1 = get_grams(S1)
    set_S2 = get_grams(S2)
    actual_similarity = jaccard_similarity(set_S1, set_S2)
    print(f"Estimated Jaccard Similarity: {estimated_similarity}")
    print(f"Actual Jaccard Similarity: {actual_similarity}")
