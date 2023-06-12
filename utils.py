from io import StringIO

import requests
from typing import List
import pandas as pd

"""
Some utility methods.
"""

"""
Simple helper method that appends a leading zero to an integer if it is single-digit and casts it to
a string.

Inputs:
    - month: An integer in the interval [1, 12].
"""
def convert(month: int) -> str:
    if month <= 9:
        return "0" + str(month)
    else:
        return str(month)


def getFirstColValue(row: List[str]) -> str:
    return row[0]

def convertDate(input: str):
    year = input[:4]
    month = input[5:7]
    day = input[8:]

    return month + "/" + day + "/" + year

def getSourceSinks():
    my_auth = ('transmission.yesapi@calpine.com', 'texasave717')
    PortfolioID = '759847'

    call1 = "https://services.yesenergy.com/PS/rest/ftr/portfolio/" + PortfolioID + "/paths.csv?"
    call_one = requests.get(call1, auth=my_auth)
    df = pd.read_csv(StringIO(call_one.text))
    df['Path'] = df['SOURCE'] + '+' + df['SINK']
    df = df[['Path', 'SOURCE', 'SINK']]
    df = df.drop_duplicates(subset=['Path'], ignore_index=True)

    # Pre-processing to convert to dictionary mapping each source to a set of sinks
    sourceSinks = {}

    for index, row in df.iterrows():
        source = row['SOURCE']
        sink = row['SINK']

        if source not in sourceSinks:
            sourceSinks[source] = set()

        sourceSinks[source].add(sink)

    return sourceSinks
