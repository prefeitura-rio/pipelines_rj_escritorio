# -*- coding: utf-8 -*-
import requests
from shapely.geometry import Point


def fetch_data(url: str, headers: dict) -> list[dict]:
    """
    Fetch data from a URL.

    Args:
        url (str): The URL to fetch data from.
        headers (dict): The headers to use in the request.

    Returns:
        list[dict]: The fetched data.
    """
    response = requests.get(url, headers=headers)
    return response.json()["data"]


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
