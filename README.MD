# 2023 Calpine Summer Internship Projects

## Description

A large repository storing the code I have written for my various tasks & projects during my 2023 Summer Internship at Calpine. For security purposes, most input & output data
files are not included in this repository. However, the code and reports are essentially unchanged.

## Usage

This code is intended to run on the Calpine Network's CA drive. Currently, all of the scripts, output data, and reports should be located at 
Y:\11_Transmission Analysis\ERCOT\101 - Misc\CRR Limit Aggregates. If, for some reason, this folder no longer exists, clone the contents into
a new repository and run from there. Please note that some scripts only work correctly when run from the intended folder - change the File I/O 
if necessary.

## Projects

A brief description of all the major projects and tasks in this repository:
  - MIS Downloading
      - An automated Python-based schematic designed to download new ERCOT (Electric Reliability Council of Texas) Market Information System (MIS) data on a daily basis. The schematic utilizes our ERCOT API to efficiently,         securely, and comprehensively request and retrieve all necessary data for analysis and decision-making. With advantages over the previous Excel VBA downloader, including detailed documentation, robust error-checking         procedures, enhanced error-handling capabilities, and significant speedups of 7-8 times faster, the project ensures timely and reliable data retrieval.
  - Network Topology
      - A computationally intense project that maps 500 KV CAISO Buses between CRR (Congestion Revenue Rights) and WECC (Western Electricity Coordinating Council) planning cases based on the similarity of their names, 
        neighboring line ratings, and topology. The project utilizes Graph algorithms, Recursion, Natural Language Processing, and the Hungarian Algorithm in order to generate nuanced comparisons between Buses.
  - Gas Balancing
  - Aggregation Tasks

## Contact

For questions or inquiries, please contact:

- Author: Stephen Xu
- Email: stephenming228@gmail.com
- GitHub: https://github.com/stephenxu10
