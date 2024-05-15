import sys
import time
import datetime
import dt.user
import dt.excel
import numpy as np

addin = dt.excel.Addin('dt-excel', 'This is a very cool simple Excel addin as always!', min_update_interval=0.01)

@addin.expose(description='Adds 2 numbers', help='To add 2 numbers give them to the function')
def add(a: int, b: int) -> float:
    return a + b

@addin.expose(description='Subtracts 2 numbers', help='To subtract 2 numbers give them to the function')
def sub(a: int, b: int) -> float:
    return a - b

@addin.expose(description='Adds argument a to an array', help='Give a number to add to an array')
def add_array(a: int) -> list:
    return [[11 + a, 34 + a]]

@addin.expose(description='Adds 2 arrays', help='Give 2 arrays for component wise add')
def add_two_arrays(a: list, b: list) -> list:
    if len(a) != len(b):
        raise ValueError('Not equal length')
    
    result = []
    for count in range(len(a)):
        result.append([])
        for count2 in range(len(a[count])):
            result[count].append(a[count][count2] + b[count][count2])

    return result

@addin.expose(description='Ticks a new value every 500ms', help='Give a seed value to the function', streaming=True)
def tick(queue: dt.excel.Queue, a: int) -> float:
    while True:
        a += 1
        queue.result(a)
        time.sleep(0.5)

@addin.expose(description='Ticks a new value every 1s', help='Give a seed value to the function', streaming=True)
def tick_minus(queue: dt.excel.Queue, a: int) -> float:
    while True:
        a -= 1
        queue.result(a)
        time.sleep(1)


@addin.expose(description="Streams a matrix of random numbers", help='Provide the dimensions of the random matrix', streaming=True)
def random_matrix(queue: dt.excel.Queue, m: int, n: int) -> list:
    X = np.random.rand(m, n)
    num_cells_to_change = min(m, n)
    while True:
        random_rows = np.random.choice(m, num_cells_to_change, replace=False)
        random_cols = np.random.choice(n, num_cells_to_change, replace=False)
        for row, col in zip(random_rows, random_cols):
            X[row, col] = np.random.rand() 
        queue.result(X.tolist())
        time.sleep(0.1)

@addin.expose(description='Stream a random price process', help='Provide the initial price', streaming=True)
def stream_price(queue: dt.excel.Queue, p: float) -> list:
    now = datetime.datetime.now()
    N = 1000
    μ, σ = 0.1, 0.2
    freq = 1
    time.sleep(np.random.exponential(freq))
    scale = N * 365 * 24 * 60 * 60 / freq
    returns = np.random.normal(loc=μ / scale, scale=σ / np.sqrt(scale), size=N)
    prices = (p * np.exp(returns.cumsum())).tolist()
    times = [(now + datetime.timedelta(seconds=t)).strftime('%x %X') for t in range(1000)]
    while True:
        ret = np.random.normal(loc=μ / scale, scale=σ / np.sqrt(scale), size=1)
        new_price = prices[-1] * (1 + ret[0]) 
        new_time = (datetime.datetime.strptime(times[-1], '%x %X') + datetime.timedelta(seconds=1)).strftime('%x %X')
        prices = prices[1:] + [new_price]
        times = times[1:] + [new_time]
        queue.result([[t, p] for t, p in zip(times, prices)])
        time.sleep(0.1)

@addin.expose(description='Get addin version', help='Display the version of the addin')
def version(_: int) -> str:
    return '0.1.5'

@addin.expose(description='Splits the string by comma and returns the nth element', help='Splits the string by comma and returns the nth element')
def split_string(string: str) -> list:
    return [string.replace(' ', '').split(',')]

def __excel_main__(port, debug=False):
     addin.run(port=port)

if __name__ == '__main__':
    __excel_main__(12346)
