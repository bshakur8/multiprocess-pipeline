import random
import time
import unittest

from multiprocess_pipeline import MultiProcessPipeline, logger

randomization = False


def start(item):
    if random.choice(range(10)) in (1, 2):
        time.sleep(0.3)
    logger.info(f"start handling {item}")
    return item, random.choice(range(10)) if randomization else item


def process(data):
    item, value = data
    if random.choice(range(10)) in (1, 2, 3, 4):
        time.sleep(0.1)
    logger.info(f"processing {item} with value {value}")
    if randomization:
        return (item,) * 3
    return item, random.choice(range(50, 200)), random.choice([True, False])


def report(data):
    if random.choice(range(10)) in (1, 2):
        time.sleep(0.4)
    item, result, info = data
    logger.info(f"Reporting {item}: result: {result}. additional info={info}")
    return random.choice(range(500, 1000)) if randomization else item


class PipelineTests(unittest.TestCase):

    def test_sanity(self):
        items = list(range(1, 3))
        multi = MultiProcessPipeline([(start, 1), (process, 1), (report, 1)], collection=items)
        multi.start()
        multi.join(ignore_results=True)

    def _test_long(self):
        items = list(range(1, 500 + 1))
        multi = MultiProcessPipeline([(start, 10), (process, 10), (report, 10)], collection=items)
        multi.start()
        multi.join(ignore_results=True)

    def test_sorted(self):
        global randomization
        orig_value = randomization
        try:
            randomization = False
            items = list(range(1, 100 + 1))
            multi = MultiProcessPipeline([(start, 3), (process, 4), (report, 2)], collection=items)
            multi.start()
            results = multi.join()

            results = list(results)
            logger.info(results)
            assert results == items
        finally:
            randomization = orig_value
