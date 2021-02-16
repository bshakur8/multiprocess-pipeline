import random
import sys
import time

from multiprocess_pipeline import MultiProcessPipeline, logger


def start(item):
    if random.choice(range(10)) in (1, 2):
        time.sleep(0.5)
    logger.info(f"start handling {item}")
    return item, random.choice(range(10))


def process(data):
    item, value = data
    if random.choice(range(10)) in (1, 2, 3, 4):
        time.sleep(1)
    logger.info(f"processing {item} with value {value}")
    return item, random.choice(range(50, 200)), random.choice([True, False])


def report(data):
    if random.choice(range(10)) in (1, 2):
        time.sleep(1)
    item, result, info = data
    logger.info(f"Reporting {item}: result: {result}. additional info={info}")
    return random.choice(range(500, 1000))


def ex1():
    num, items = 10, list(range(1, 50 + 1))
    MultiProcessPipeline([(start, num), (process, num), (report, num)], collection=items)()


def ex2():
    items = list(range(1, 1000 + 1))
    multi = MultiProcessPipeline([(start, 1), (process, 2), (report, 3)], collection=items)
    multi.start()
    multi.join()


def main():
    ex1()
    # ex2()


if __name__ == '__main__':
    sys.exit(main())
