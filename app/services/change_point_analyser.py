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

def fetch_metrics(start_time: int, end_time: int, data_type: str, metric: str) -> pd.DataFrame:
    """
    Fetch raw metrics from DB; build a DataFrame indexed by endtime for the given metric.
    Expects raw list of dicts with 'starttime', 'endtime', and 'data'.
    Sorts entries by endtime ascending.
    """
    raw = get_metrics_within_time_range(start_time, end_time)
    if not raw:
        return pd.DataFrame()

    records = []
    for entry in raw:
        end_ts = entry.get('end_time', entry.get('endtime', 0))
        ts = pd.to_datetime(int(end_ts) / 1e6, unit='s')

        entry_data = entry.get('data', {})
        data_list = entry_data.get(data_type, [])

        if data_type == 'edges':
            if metric == 'freq':
                val = sum(item.get('frequency', 0) for item in data_list)
            elif metric == 'lat':
                vals = [item.get('latency', 0.0) for item in data_list]
                val = float(np.mean(vals)) if vals else 0.0
            elif metric in ('coexec', 'coexecution'):
                vals = [
                    item.get('coexecution', item.get('coexec', 0.0))
                    for item in data_list
                ]
                val = float(np.mean(vals)) if vals else 0.0
            else:
                raise ValueError(f"Invalid edge metric '{metric}'")
        elif data_type == 'nodes':
            if metric == 'imp':
                vals = [node.get('absoluteimportance', 0.0) for node in data_list]
                val = float(np.mean(vals)) if vals else 0.0
            elif metric == 'dep':
                vals = [node.get('absolutedependence', 0.0) for node in data_list]
                val = float(np.mean(vals)) if vals else 0.0
            else:
                raise ValueError(f"Invalid node metric '{metric}'")
        else:
            raise ValueError(f"Invalid data_type '{data_type}'")
        records.append({'time': ts, metric: val})

    df = pd.DataFrame(records).set_index('time').sort_index()
    return df

def fetch_node_metrics(
    metric: str,
    node_id: str = None,
    raw: List[dict] = None
) -> pd.DataFrame:
    """
    Fetch node-level metrics over time.
    
    Parameters
    ----------
    start_time, end_time : int
        Query window (timestamps in microseconds).
    metric : {'imp', 'dep'}
        'imp' → absoluteimportance, 'dep' → absolutedependence.
    node_id : str, optional
        If given, filters to that node's series; otherwise aggregates across all nodes.
    """
    if not raw:
        return pd.DataFrame()
    records = []
    for entry in raw:
        ts = pd.to_datetime(int(entry.get('end_time', entry.get('endtime', 0))) / 1e6, unit='s')
        nodes = entry.get('data', {}).get('nodes', [])
        if node_id:
            nodes = [n for n in nodes if n.get('id') == node_id]

        if metric == 'imp':
            vals = [n.get('absoluteimportance', 0.0) for n in nodes]
        elif metric == 'dep':
            vals = [n.get('absolutedependence', 0.0) for n in nodes]
        else:
            raise ValueError(f"Invalid node metric '{metric}'")

        val = float(np.mean(vals)) if vals else 0.0
        records.append({'time': ts, metric: val})

    return pd.DataFrame(records).set_index('time').sort_index()

def fetch_edge_metrics(
    metric: str,
    source: str = None,
    target: str = None,
    raw: List[dict] = None
) -> pd.DataFrame:
    """
    Fetch edge-level metrics over time.
    
    Parameters
    ----------
    start_time, end_time : int
        Query window (timestamps in microseconds).
    metric : {'freq', 'lat', 'coexec'}
        'freq' → sum of frequency,
        'lat'  → mean latency,
        'coexec' → mean coexecution.
    source, target : str, optional
        If both given, filters to that edge; otherwise aggregates across all edges.
    """
    if not raw:
        return pd.DataFrame()
    records = []
    for entry in raw:
        ts = pd.to_datetime(int(entry.get('end_time', entry.get('endtime', 0))) / 1e6, unit='s')
        edges = entry.get('data', {}).get('edges', [])
        if source and target:
            edges = [e for e in edges if e.get('source') == source and e.get('target') == target]

        if metric == 'freq':
            val = sum(e.get('frequency', 0) for e in edges)
        elif metric == 'lat':
            latencies = [e.get('latency', 0.0) for e in edges]
            val = float(np.mean(latencies)) if latencies else 0.0
        elif metric in ('coexec', 'coexecution'):
            coexe = [e.get('coexecution', e.get('coexec', 0.0)) for e in edges]
            val = float(np.mean(coexe)) if coexe else 0.0
        else:
            raise ValueError(f"Invalid edge metric '{metric}'")

        records.append({'time': ts, metric: val})

    return pd.DataFrame(records).set_index('time').sort_index()

def detect_change_points(signal: np.ndarray, penalty: float = 10.0) -> List[int]:
    """
    Detect change points in a 1-D signal array using the PELT algorithm.
    """
    if len(signal) < 2:
        return [0, len(signal)]
    algo = rpt.Pelt(model="rbf").fit(signal)
    return algo.predict(pen=penalty)

def build_response(df: pd.DataFrame, metric: str, bkps: List[int]) -> JSONResponse:
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

    edges = [
        {"source": src, "target": tgt}
        for src, tgt in pairs
    ]
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
    nodes = get_nodes(raw=raw)
    directed_edges = get_all_edges(nodes, directed=True)

    df = fetch_metrics(start_time, end_time, data_type, metric)
    node_cps = analyze_node_change_points(raw, metric="imp", node_ids=nodes)
    edge_cps = analyze_edge_change_points(raw, metric="freq", edges=directed_edges)
    print(f"Node change points: {node_cps}")
    print(f"Edge change points: {edge_cps}")
    
    if df.empty or metric not in df.columns:
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": f"No data for metric '{metric}' in given range."}
        )

    signal = df[metric].astype(float).values
    print(signal)
    bkps = detect_change_points(signal, penalty=10)
    return build_response(df, metric, bkps)

def _detect_change_points(
    series: pd.Series,
    model: str = "l2",
    pen: float = 3.0
) -> List[pd.Timestamp]:
    """
    Run PELT change-point detection on a 1D series.
    Returns the list of timestamps (excluding the very end).
    """
    algo = rpt.Pelt(model=model).fit(series.values)
    change_idxs = algo.predict(pen=pen)[:-1]
    return [series.index[i] for i in change_idxs]


def analyze_node_change_points(
    raw: List[Dict[str, Any]],
    metric: str,
    node_ids: List[str],
    model: str = "l2",
    pen: float = 3.0
) -> Dict[str, List[pd.Timestamp]]:
    """
    For each node_id in node_ids, fetch its metric series and detect change points.
    Returns a dict: node_id -> list of Timestamps.
    """
    results: Dict[str, List[pd.Timestamp]] = {}
    for nid in node_ids:
        print(f"Analyzing node {nid} for metric {metric}")
        df = fetch_node_metrics(metric=metric, node_id=nid, raw=raw)
        if df.empty:
            results[nid] = []
        else:
            results[nid] = _detect_change_points(df[metric], model=model, pen=pen)
    return results


def analyze_edge_change_points(
    raw: List[Dict[str, Any]],
    metric: str,
    edges: List[Dict[str, str]],
    model: str = "l2",
    pen: float = 3.0
) -> Dict[Tuple[str, str], List[pd.Timestamp]]:
    """
    For each edge in edges (must have 'source' & 'target'), detect change points.
    Returns a dict: (source, target) -> list of Timestamps.
    """
    results: Dict[Tuple[str, str], List[pd.Timestamp]] = {}
    for e in edges:
        src, tgt = e["source"], e["target"]
        df = fetch_edge_metrics(metric=metric, source=src, target=tgt, raw=raw)
        key = (src, tgt)
        if df.empty:
            results[key] = []
        else:
            results[key] = _detect_change_points(df[metric], model=model, pen=pen)
    return results
