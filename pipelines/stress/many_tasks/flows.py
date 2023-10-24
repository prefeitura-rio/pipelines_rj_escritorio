# -*- coding: utf-8 -*-
from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants
from pipelines.stress.many_tasks.tasks import (
    cast_to_float,
    compare_results_with_delta,
    divide_numbers,
    log_variable,
    multiply_numbers,
    subtract_numbers,
    sum_numbers,
)

with Flow(
    name="Stress Test: Many Tasks",
    state_handlers=[handler_inject_bd_credentials],
) as stress__many_tasks__main_flow:
    # Parameters
    a = Parameter("a")
    b = Parameter("b")

    # Convert them to floats
    a_float = cast_to_float(a)
    b_float = cast_to_float(b)

    # Make c = (a + b) * (a - b)
    sum_ab = sum_numbers(a_float, b_float)
    sub_ab = subtract_numbers(a_float, b_float)
    c = multiply_numbers(sum_ab, sub_ab)

    # Get a^2 and b^2
    a_squared = multiply_numbers(a_float, a_float)
    b_squared = multiply_numbers(b_float, b_float)

    # Make d = c / (a^2 - b^2)
    d = divide_numbers(c, subtract_numbers(a_squared, b_squared))

    # d must be close to 1
    is_close_to_one = compare_results_with_delta(d, 1.0)

    # Now we print out all the variables
    log_variable("a", a)
    log_variable("b", b)
    log_variable("a_float", a_float)
    log_variable("b_float", b_float)
    log_variable("sum_ab", sum_ab)
    log_variable("sub_ab", sub_ab)
    log_variable("c", c)
    log_variable("a_squared", a_squared)
    log_variable("b_squared", b_squared)
    log_variable("d", d)
    log_variable("is_close_to_one", is_close_to_one)


# Storage and run configs
stress__many_tasks__main_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
stress__many_tasks__main_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
