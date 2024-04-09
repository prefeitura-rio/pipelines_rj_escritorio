# -*- coding: utf-8 -*-
import json

from google.cloud.firestore_v1.client import Client as FirestoreClient
from shapely.geometry import Point
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon


def get_bairros_with_geometry(db: FirestoreClient):
    bairros = [doc.to_dict() for doc in db.collection("bairro").stream()]
    for bairro in bairros:
        bairro["geo"] = json.loads(bairro["geo"])
        if bairro["geo"]["geometry"]["type"] == "Polygon":
            bairro["polygon"] = Polygon(bairro["geo"]["geometry"]["coordinates"][0])
        elif bairro["geo"]["geometry"]["type"] == "MultiPolygon":
            bairro["polygon"] = MultiPolygon(
                [Polygon(coords[0]) for coords in bairro["geo"]["geometry"]["coordinates"]]
            )
        else:
            raise ValueError(f"Invalid geometry type {bairro['geo']['geometry']['type']}")
    return bairros


def get_bairro_from_lat_long(lat: float, long: float, bairros: list):
    point = Point(long, lat)
    for bairro in bairros:
        if bairro["polygon"].contains(point):
            return bairro
    raise ValueError(f"Could not find bairro for ({lat},{long})")


def to_snake_case(val: str):
    if not val:
        return val
    return val.strip().lower().replace(" ", "_")


def to_camel_case(val: str):
    if not val:
        return val
    return val.strip().title().replace("_", " ")
