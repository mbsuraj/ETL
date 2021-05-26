"""Provide a simple data catalog to act as
the single point of truth for the location of
data.

This catalog assumes the data lake is filesystem based.
In realistic situations, it does not have to be.

TODO: improve by making an abstraction of the
 type of the data (database, file: csv/parquet/…, …)
"""

from src.config import DATA_LAKE


def _resource(zone, key):
    return str(DATA_LAKE / zone / key)


catalog = {
    "clean/holdout": _resource("clean", "holdout"),
    "clean/composite": _resource("clean", "composite"),
    "landing/event_attributes": _resource("landing", "event_attributes.csv"),
    "landing/holdout": _resource("landing", "HMAHCC_HOLDOUT.csv"),
    "landing/composite": _resource("landing", "HMAHCC_COMP.csv"),
}
