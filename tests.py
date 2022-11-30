import json
import unittest
from multiprocessing import get_context
from multiprocessing.queues import Queue

import tasks
from api_client import YandexWeatherAPI
from data_for_test.data_tests import (
    best_city,
    result_agregation,
    result_calculation,
    url_test,
)
from forecasting import forecast_weather

forecast_weather()


class TestDataCalculationTask(unittest.TestCase):
    ywAPI = YandexWeatherAPI()
    ctx = get_context("forkserver")
    queue = Queue(ctx=ctx)
    dc = tasks.DataCalculationTask()
    df = tasks.DataFetchingTask(ywAPI, url_test, queue)
    da = tasks.DataAggregationTask(queue, dc.get_data_calculation)

    def test_fetching_and_calculation(self) -> None:
        """Тест задач DataFetchingTask, DataCalculationTask"""
        self.df.get_data_fetching()
        result = self.dc.get_data_calculation(self.queue.get(block=True, timeout=0))
        self.assertEqual(
            result,
            result_calculation,
            msg="Полученный результат из функции - не верный",
        )

    def test_aggregation(self) -> None:
        self.da.get_data_aggregation()
        with open("Aggregation_data.json", "r") as file_check:
            result = json.load(file_check)
            self.assertEqual(
                result[0],
                result_agregation,
                msg="Объединенные данные не совпадают с шаблоном",
            )

    def test_analysis(self) -> None:
        with open("Cities_raiting.json", "r") as file_check:
            check = json.load(file_check)[0]["city"]
            self.assertEqual(best_city, check, msg="Не совпадает ТОП-город для отдыха")


if __name__ == "__main__":
    unittest.main()
