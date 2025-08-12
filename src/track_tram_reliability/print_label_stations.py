from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

from .gtfs_index import load_label_index


def resolve_stations_for_labels(index_path: Path, products: Iterable[str], labels: Iterable[str]) -> List[str]:
    index = load_label_index(index_path)
    out = set()
    prods = {p.strip().upper() for p in products}
    labs = {l.strip().upper() for l in labels}
    # Always include matches from requested products and from ALL bucket
    for p in prods:
        prod_map = index.mapping.get(p, {})
        for l in labs:
            out.update(prod_map.get(l, []))
    all_map = index.mapping.get("ALL", {})
    for l in labs:
        out.update(all_map.get(l, []))
    return sorted(out)
