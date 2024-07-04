# -*- coding: utf-8 -*-
import re
from copy import deepcopy
from typing import List, Tuple

import requests
from shapely.geometry import Point
from unidecode import unidecode


def clean_string(input_string):
    no_accents = unidecode(input_string)
    lower_case = no_accents.lower()
    cleaned_string = re.sub(r"\s+", " ", lower_case).strip()
    return cleaned_string


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


def from_to_theme_program(
    previous_theme: str,
    previous_program: str,
    possible_themes: List[str],
) -> Tuple[str, str]:
    """
    Maps the previous theme and program to the new theme and program.

    Args:
        previous_theme (str): The previous theme.
        previous_program (str): The previous program.

    Returns:
        Tuple[str, str]: The new theme and program.
    """
    original_theme = deepcopy(previous_theme)
    original_program = deepcopy(previous_program)
    original_possible_themes = deepcopy(possible_themes)
    previous_theme = clean_string(previous_theme)
    previous_program = clean_string(previous_program)
    possible_themes = [clean_string(theme) for theme in possible_themes]
    if previous_theme in possible_themes:
        return original_possible_themes[possible_themes.index(previous_theme)], None
    if previous_theme == "saude":
        return "Saúde", None
    elif previous_theme.startswith("educacao"):
        return "Educação", None
    elif previous_theme == "protecao social":
        return "Proteção Social", None
    elif previous_theme == "renovacao urbana e infraestrutura":
        if previous_program == "porto maravilha":
            return "Desenvolvimento Econômico", None
        elif previous_program in [
            "parque madureira",
            "bairro maravilha",
            "entorno engenhao/praca do trem",
        ]:
            return "Obras e Infraestrutura", None
        elif previous_program == "piscinoes da tijuca":
            return "Zeladoria", None
    elif previous_theme == "habilitacao":
        return "Proteção Social", None
    elif previous_theme == "acoes sociais e direitos humanos":
        if previous_program == "casas da mulher":
            return "Representatividade", None
    elif previous_theme == "legado olimpico e esporte":
        if previous_program == "equipamentos olimpicos":
            return "Legado Olímpico", None
        elif previous_program == "vilas olimpicas":
            return "Esporte", None
        elif previous_program.startswith("rio ar livre"):
            return "Representatividade", None
    elif previous_theme == "meio ambiente":
        if previous_program == "reflorestamento":
            return "Meio Ambiente", None
        elif previous_program.startswith("novos parques"):
            return "Obras e Infraestrutura", None
    elif previous_theme == "tecnologia":
        if previous_program.startswith("centro de operacoes"):
            return "Resiliência Climática", None
        elif previous_theme == "nave e pracas do conhecimento":
            return "Rios do Futuro"
    elif previous_theme == "cultura":
        return "Turismo e Cultura", None
    elif previous_theme.startswith("turismo"):
        if previous_program.startswith("espacos novos ou revitalizados"):
            return "Legado Olímpico", None
    raise ValueError(f"Could not map theme and program for ({original_theme}, {original_program})")


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
