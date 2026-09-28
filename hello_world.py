import flyte

env = flyte.TaskEnvironment(name="hello_world")

@env.task
def fn(x: int) -> int:
    slope, intercept = 2, 5
    return slope * x + intercept

@env.task
def main(n: int) -> float:
    y_list = list(flyte.map(fn, range(n)))
    return sum(y_list) / len(y_list)