from fastapi import Query
from fastapi.responses import JSONResponse
import numpy as np
import pandas as pd
import ruptures as rpt
from typing import List
from app.services.db_service import get_metrics_within_time_range
from typing import List, Tuple, Dict, Any
from itertools import permutations, combinations

def classify_metric(metric: str) -> str:
    """
    Determine if the provided metric is a node or edge metric.
    """
    node_metrics = ["imp", "dep"]
    edge_metrics = ["lat", "freq", "coexec"]
    if metric in node_metrics:
        return "nodes"
    if metric in edge_metrics:
        return "edges"
    raise ValueError(f"Invalid metric '{metric}'")

def fetch_node_metrics(raw, metric: str, node_id: str = None) -> pd.DataFrame:
    """
    Fetch raw node metrics from DB; build a DataFrame indexed by endtime for the given metric.
    Expects raw list of dicts with 'starttime', 'endtime', and 'data'.
    Sorts entries by endtime ascending. Accepts node_id for filtering node-specific data.
    """
    records = []
    for entry in raw:
        end_ts = entry.get('end_time', entry.get('endtime', 0))
        ts = pd.to_datetime(int(end_ts) / 1e6, unit='s')

        nodes = entry.get('data', {}).get('nodes', [])
        if node_id:
            nodes = [node for node in nodes if node.get('id') == node_id]
        if nodes is None or nodes == []:
            continue
        
        if metric == 'imp':
            vals = [node.get('absoluteimportance', 0.0) for node in nodes]
            val = float(np.mean(vals)) if vals else 0.0
        elif metric == 'dep':
            vals = [node.get('absolutedependence', 0.0) for node in nodes]
            val = float(np.mean(vals)) if vals else 0.0
        else:
            raise ValueError(f"Invalid node metric '{metric}'")

        records.append({'time': ts, metric: val})

    df = pd.DataFrame(records).set_index('time').sort_index()
    return df

def fetch_edge_metrics(raw, metric: str, source: str = None, target: str = None) -> pd.DataFrame:
    """
    Fetch raw edge metrics from DB; build a DataFrame indexed by endtime for the given metric.
    Expects raw list of dicts with 'starttime', 'endtime', and 'data'.
    Sorts entries by endtime ascending. Accepts source and target for filtering edge-specific data.
    """
    records = []
    for entry in raw:
        # print(f"Processing entry: {entry}")
        end_ts = entry.get('end_time', entry.get('endtime', 0))
        ts = pd.to_datetime(int(end_ts) / 1e6, unit='s')

        edges = entry.get('data', {}).get('edges', [])
        if source and target:
            edges = [edge for edge in edges if edge.get('source') == source and edge.get('target') == target]
        if edges is None or edges == []:
            continue

        if metric == 'freq':
            val = sum(edge.get('frequency', 0) for edge in edges)
        elif metric == 'lat':
            vals = [edge.get('latency', 0.0) for edge in edges]
            val = float(np.mean(vals)) if vals else 0.0
        elif metric in ('coexec', 'coexecution'):
            vals = [
                edge.get('coexecution', edge.get('coexec', 0.0))
                for edge in edges
            ]
            val = float(np.mean(vals)) if vals else 0.0
        else:
            raise ValueError(f"Invalid edge metric '{metric}'")
        
        print(f"Adding record for {ts}: {metric} = {val}")
        records.append({'time': ts, metric: val})

    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records).set_index('time').sort_index()
    return df

def detect_change_points(signal: np.ndarray, penalty: float = 10.0) -> List[int]:
    """
    Detect change points in a 1-D signal array using the PELT algorithm.
    """
    if len(signal) < 2:
        return [0, len(signal)]
    algo = rpt.Pelt(model="rbf").fit(signal)
    return algo.predict(pen=penalty)

def build_response(df: pd.DataFrame, metric: str, bkps: List[int]) -> Dict[str, Any]:
    """
    Build JSONResponse with series and change points, using timestamp index.
    """
    series = [{"time": idx.isoformat(), metric: float(val)}
              for idx, val in zip(df.index, df[metric].astype(float).values)]
    change_points = [df.index[idx].isoformat() for idx in bkps[:-1]]
    return {"series": series, "change_points": change_points}

def get_nodes(raw):
    """
    Extracts and concatenates all 'nodes' lists from the raw response.
    
    Parameters
    ----------
    raw : list of dict
        The JSON‐decoded response.
    
    Returns
    -------
    list of dict
        All node dicts flattened into one list.
    """
    nodes = []
    for entry in raw:
        nodes.extend(entry.get("data", {}).get("nodes", []))
    return nodes

def get_all_edges(
    nodes: List[Dict[str, any]],
    directed: bool = True
) -> List[Dict[str, str]]:
    """
    Generate every possible edge between the given nodes.

    Parameters
    ----------
    nodes : List[Dict]
        A list of node dicts, each having at least an 'id' key.
    directed : bool, default True
        If True, returns all ordered pairs (i→j, i != j).
        If False, returns each unordered pair once (i—j).

    Returns
    -------
    edges : List[Dict[str, str]]
        A list of edge dicts with 'source' and 'target'.
    """
    ids = [node["id"] for node in nodes]

    if directed:
        pairs = permutations(ids, 2)
    else:
        pairs = combinations(ids, 2)

    seen = set()
    edges = []
    for src, tgt in pairs:
        if (src, tgt) not in seen:
            edges.append({"source": src, "target": tgt})
            seen.add((src, tgt))
    return edges

def handle_detect_change_points(
    start_time: int = Query(..., description="Start of time range, epoch micros"),
    end_time: int = Query(..., description="End of time range, epoch micros"),
    metric: str = Query(..., description="Metric to analyze: absolute_importance, absolute_dependence, latency, frequency, coexecution")
    ) -> JSONResponse:
    """
    API endpoint: detect change points for a given metric over a time range.
    """
    try:
        data_type = classify_metric(metric)
    except ValueError as e:
        print(f"ERROR: {str(e)}")
        return JSONResponse(status_code=400, content={"status": "error", "message": str(e)})

    raw = get_metrics_within_time_range(start_time, end_time)
    if not raw:
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": "No data found for the given time range."}
        )
    if data_type == "edges" and metric not in ["lat", "freq", "coexec"]:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": f"Invalid edge metric '{metric}'"}
        )
    if data_type == "nodes" and metric not in ["imp", "dep"]:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": f"Invalid node metric '{metric}'"}
        )
    
    nodes = get_nodes(raw=raw)
    results = []
    if data_type == "edges":
        directed_edges = get_all_edges(nodes, directed=True)
        for edge in directed_edges:
            print(f"Processing edge {edge['source']} -> {edge['target']}")
            df = fetch_edge_metrics(raw, metric, source=edge['source'], target=edge['target'])
            if df.empty or metric not in df.columns:
                print(f"No data for edge {edge['source']} -> {edge['target']} with metric '{metric}'")
                continue
            signal = df[metric].astype(float).values
            bkps = detect_change_points(signal, penalty=10)
            results.append(
                build_response(df, metric, bkps) | {"source": edge['source'], "target": edge['target']}
            )
    elif data_type == "nodes":
        for node in nodes:
            df = fetch_node_metrics(raw, metric, node_id=node['id'])
            if df.empty or metric not in df.columns:
                print(f"No data for node {node['id']} with metric '{metric}'")
                continue
            print(f"Processing node {node['id']}")
            signal = df[metric].astype(float).values
            bkps = detect_change_points(signal, penalty=10)
            results.append(
                build_response(df, metric, bkps) | {"node": node['id']}
            )
    
    if results == []:
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": f"No change points detected for metric '{metric}' in the given time range."}
        )
    
    response_content = {
        "status": "success",
        "message": f"Change points detected for metric '{metric}'",
        "data": results
    }
    return JSONResponse(status_code=200, content=response_content)
    
