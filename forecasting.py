import logging
from multiprocessing import get_context
from multiprocessing.queues import Queue

from api_client import YandexWeatherAPI
from tasks import (
    DataAggregationTask,
    DataAnalyzingTask,
    DataCalculationTask,
    DataFetchingTask,
)
from utils import CITIES


def forecast_weather() -> None:
    """
    Анализ погодных условий по городам
    """
    ywAPI = YandexWeatherAPI()
    ctx = get_context("forkserver")
    queue = Queue(ctx=ctx)
    logging.info("Переходим на новый уровень")
    fetching_task = DataFetchingTask(ywAPI, CITIES, queue)
    calculation_task = DataCalculationTask()
    aggregation_task = DataAggregationTask(queue, calculation_task.get_data_calculation)
    analyzing_task = DataAnalyzingTask(queue)
    fetching_task.get_data_fetching()
    aggregation_task.get_data_aggregation()
    result = analyzing_task.get_analyzed_data()
    logging.info(result)


if __name__ == "__main__":
    forecast_weather()
