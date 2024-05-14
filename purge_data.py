from ast import parse
import re
import pathlib
import pandas as pd
import numpy as np
from functools import reduce

RAW_DATASET_PATH = pathlib.Path("./data/raw")
PROCESSED_DATASET_PATH = pathlib.Path("./data/proc")

def parse_input_line(line: str) -> tuple[list[str], list[tuple[int, int]]]:
    """Parse a line of SAS code
    Args:
    line: a line of SAS code

    Returns:
    a tuple with the name of variables.
    """
    var_name, width_tup = line.split()
    if width_tup.find("-") != -1:
        width_split = width_tup.split("-")
        start = int(width_split[0]) - 1
        end = int(width_split[1])
    else:
        start = int(width_tup) - 1
        end = int(width_tup)
    return ([var_name], [(start, end)])

def parse_sas_input(input_section: list[str]) -> tuple[list[str], list[tuple[int, int]]]:
    """a parser function of the SAS code
    Args:
    input_section: a list including lines of SAS code

    Returns:
    a tuple with the first element as label and the second element as the indeces
    """
    return list(reduce(
        lambda x, y: (x[0] + y[0], x[1] + y[1]),
        map(parse_input_line, input_section))) # type: ignore

def parse_label_line(label_line:str) -> tuple[str, str]:
    """a regex based function to parse the label line of SAS code
    Args:
    label_line: a SAS code line with label function

    Returns:
    a tuple with the label and label information
    """
    pat = re.compile('(\w+)\s+=\s\"(.*)\"')
    return (pat.search(label_line).group(1), pat.search(label_line).group(2))

def parse_label_lines(label_section: list[str]) -> pd.Series:
    return pd.Series(
        dict(map(parse_label_line, label_section))
    )

def parse_ds(ds_name: str) -> pd.DataFrame:
    """a parser function for the analysis of fix width files"""
    with open(RAW_DATASET_PATH.joinpath(f'{ds_name}.sas')) as f:
        lines = [i.strip() for i in f.readlines()]
    idx_input_start = lines.index("INPUT") + 1
    idx_input_end = lines.index("LABEL") - 2
    names, colspecs = parse_sas_input(lines[idx_input_start:idx_input_end])
    ds = pd.read_fwf(RAW_DATASET_PATH.joinpath(f'{ds_name}.dat'), names = names, colspecs = colspecs)
    return ds

def parse_meta(ds_name: str) -> pd.Series:
    with open(RAW_DATASET_PATH.joinpath(f'{ds_name}.sas')) as f:
        lines = [i.strip() for i in f.readlines()]
    idx_label = lines.index("LABEL")
    return parse_label_lines(lines[idx_label + 1 : -3])    
