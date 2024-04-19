# -*- coding: utf-8 -*-
from shapely.geometry import Point


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
