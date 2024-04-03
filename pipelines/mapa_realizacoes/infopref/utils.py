# -*- coding: utf-8 -*-
def to_snake_case(val: str):
    if not val:
        return val
    return val.strip().lower().replace(" ", "_")


def to_camel_case(val: str):
    if not val:
        return val
    return val.strip().title().replace("_", " ")
