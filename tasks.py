import json
import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, cpu_count
from multiprocessing.queues import Queue
from typing import Callable

from api_client import YandexWeatherAPI
from utils import BEST_CONDITION, TRANSLATE_CITIES

logging.basicConfig(
    level="DEBUG",
    filename="logfile_tasks.log",
    filemode="w",
    format=(
        "%(asctime)s [%(levelname)s] %(name)s,"
        " %(message)s, %(filename)s, line %(lineno)d"
    ),
)
logger = logging.getLogger()


class DataFetchingTask:
    def __init__(self, ywAPI: YandexWeatherAPI, cities: dict, queue: Queue):
        self.cities = cities
        self.api = ywAPI
        self.queue = queue

    def get_data_fetching(self):
        logging.info("Начало сбора данных с API YandexWeather")
        try:
            with ThreadPoolExecutor() as pool:
                fetched_data = pool.map(self.api.get_forecasting, self.cities.keys())
                for city in fetched_data:
                    self.queue.put(city)
                self.queue.put(None)
        except Exception as error:
            logging.exception(f"Ошибка запроса API: {error}")
            raise error
        logging.info("Окончание сбора данных с API YandexWeather")


class DataCalculationTask:
    def __init__(self):
        self.best_conditions = BEST_CONDITION

    def _get_analisys_by_hours(self, hours: dict) -> tuple[int, float, int]:
        rainless_hours = 0
        sum_temp_per_day = 0.0
        full_day = 0
        for hour in hours:
            if 9 <= int(hour["hour"]) <= 19:
                full_day += 1
                sum_temp_per_day += hour["temp"]
                if hour["condition"] in self.best_conditions:
                    rainless_hours += 1
        full_day = True if full_day == 11 else False
        return rainless_hours, sum_temp_per_day, full_day

    def get_data_calculation(self, city: dict) -> list:
        result: dict = {}
        city_name = TRANSLATE_CITIES[city["geo_object"]["locality"]["name"]]
        logging.info(
            f"Вычисление средней температуры и анализ осадков в городе: {city_name}"
        )
        try:
            result["city"] = city_name
            result["days"] = []
            num_of_days = 0
            sum_avg_temps_days = 0.0
            sum_rainless_hours_days = 0
            for forecast in city["forecasts"]:
                result["days"].append({"date": forecast["date"]})
                (
                    rainless_hours,
                    sum_temp_per_day,
                    full_day_flag,
                ) = self._get_analisys_by_hours(forecast["hours"])
                if full_day_flag:
                    num_of_days += 1
                    result["days"][-1]["rainless hours"] = rainless_hours
                    avg_temp_day: float = round(sum_temp_per_day / 11, 1)
                    sum_avg_temps_days += avg_temp_day
                    sum_rainless_hours_days += rainless_hours
                    result["days"][-1]["averange temperature per day"] = avg_temp_day
            result["average temp for all days"] = round(
                sum_avg_temps_days / num_of_days, 1
            )
            result["average rainless hours for all days"] = round(
                sum_rainless_hours_days / num_of_days, 1
            )
        except Exception as error:
            logging.exception(f"Ошибка вычисления данных: {error}")
            raise error
        logging.info(f"Окончание вычислений для города: {city_name}")
        return result


class DataAggregationTask:
    def __init__(self, queue: Queue, calc_func: Callable):
        self.queue = queue
        self.calculation_func = calc_func
        self.buffer_for_result = list()

    def _callback(self, result: list) -> None:
        self.buffer_for_result.append(result)
        logging.info(
            f'Объединение полученных данных завершено для города: {result["city"]}'
        )

    def _error_callback(self, error: Exception) -> Exception:
        logging.exception(f"Ошибка объединения данных: {error}")
        raise error

    def get_data_aggregation(self) -> None:
        cores_count = cpu_count()
        logging.info("Начало получения объединенных данных")
        try:
            with Pool(processes=cores_count - 1) as pool:
                while city_data := self.queue.get(block=True, timeout=0):
                    city_name = city_data["geo_object"]["locality"]["name"]
                    logging.info(
                        f"Полученние данных о городе: {TRANSLATE_CITIES[city_name]}"
                    )
                    result = pool.apply_async(
                        self.calculation_func,
                        (city_data,),
                        callback=self._callback,
                        error_callback=self._error_callback,
                    )
                    calculated_data = result.get()
                    self.queue.put(calculated_data)
                self.queue.put(None)
            with open("Aggregation_data.json", "w+") as f:
                json.dump(
                    self.buffer_for_result,
                    f,
                    indent=1,
                    sort_keys=True,
                    ensure_ascii=False,
                )
        except Exception as error:
            logging.exception(f"Ошибка полученния данных: {error}")
        logging.info("Окончание получения объединенных данных")


class DataAnalyzingTask:
    def __init__(self, queue: Queue):
        self.queue: Queue = queue

    def get_analyzed_data(self) -> str:
        data = list()
        logging.info("Начало анализа полученных данных")
        try:
            while city_calculated_data := self.queue.get(block=True, timeout=0):
                data.append(city_calculated_data)
            data.sort(
                key=lambda x: (
                    x["average rainless hours for all days"],
                    x["average temp for all days"],
                ),
                reverse=True,
            )
            rated_cities = list()
            with open("Cities_raiting.json", "w") as f:
                best_params = list()
                best_cities = list()
                for rating, city in enumerate(data, start=1):
                    if rating == 1:
                        best_params = [
                            city["average rainless hours for all days"],
                            city["average temp for all days"],
                        ]
                        best_cities.append(city["city"])
                    else:
                        params: list = [
                            city["average rainless hours for all days"],
                            city["average temp for all days"],
                        ]
                        if params == best_params:
                            best_cities.append(city["city"])
                    city.pop("days")
                    city["rating"] = rating
                    rated_cities.append(city)
                json.dump(rated_cities, f, indent=1, sort_keys=True, ensure_ascii=False)
        except Exception as error:
            logging.exception(f"Ошибка анализа полученных данных: {error}")
            raise error
        logging.info("Окончание анализа данных")
        return f"""Наиболее благоприятные города,
                   в которых средняя температура за всё время была самой высокой,
                   а количество осадков минимальным: {", ".join(best_cities)}"""
