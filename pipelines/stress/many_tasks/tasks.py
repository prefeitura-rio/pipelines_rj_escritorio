# -*- coding: utf-8 -*-
from time import sleep
from typing import Any

from prefect import task
from prefeitura_rio.pipelines_utils.logging import log


@task
def cast_to_float(number: Any) -> float:
    sleep(0.25)
    return float(number)


@task
def sum_numbers(a: float, b: float) -> float:
    sleep(0.25)
    return a + b


@task
def multiply_numbers(a: float, b: float) -> float:
    sleep(0.25)
    return a * b


@task
def divide_numbers(a: float, b: float) -> float:
    sleep(0.25)
    return a / b


@task
def subtract_numbers(a: float, b: float) -> float:
    sleep(0.25)
    return a - b


@task
def compare_results_with_delta(a: float, b: float, delta: float = 0.0001) -> bool:
    sleep(0.25)
    return abs(a - b) < delta


@task
def log_variable(name: str, value: Any) -> None:
    log(f"{name}: {value}")
