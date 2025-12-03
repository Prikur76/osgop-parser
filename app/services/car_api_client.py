import httpx
import logging

from typing import List, Dict, Optional, Any
from httpx import RequestError, HTTPStatusError
from urllib.parse import urlencode

from app.core.config import config
from app.services.plate_normalizer import normalize_plate_for_api

logger = logging.getLogger(__name__)


class CarApiClient:
    def __init__(self, base_url: Optional[str] = None, 
                 username: Optional[str] = None, 
                 password: Optional[str] = None):
        """
        Инициализация клиента для работы с API 1С.
        """
        self.base_url = base_url or config.API_1C_BASE_URL
        self.username = username or config.API_1C_USERNAME
        self.password = password or config.API_1C_PASSWORD
        self.enabled = config.API_1C_ENABLED
        
        if not self.base_url:
            logger.warning("API 1С URL не настроен. API будет отключен.")
            self.enabled = False
        
        if self.enabled:
            self.client = httpx.Client(
                auth=(self.username, self.password),
                timeout=config.API_1C_TIMEOUT,
                verify=config.API_1C_VERIFY_SSL
            )
            logger.info(f"API 1С Client initialized for {self.base_url}")
        else:
            self.client = None
            logger.info("API 1С Client disabled")

    def get_cars_with_filters(self,
                              num: Optional[str] = None,
                              inn: Optional[str] = None,
                              vin: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получает список машин из API с фильтрами.
                
        :param vin: VIN (опционально)
        :param num: Госномер (опционально)
        :param inn: ИНН организации (опционально)
        :return: Список машин
        """
        if not self.enabled:
            logger.warning("API 1С отключен. Пропускаем запрос.")
            return []

        endpoint = "/hs/Car/v1/Get"
        url = self.base_url.rstrip("/") + endpoint
        
        # Формируем параметры запроса
        params = {}
        if inn:
            params["inn"] = inn
        if vin:
            params["vin"] = vin
        if num:
            num_normalized = normalize_plate_for_api(num)
            params["num"] = num_normalized
            logger.debug(f"Нормализован номер для API: '{num}' -> '{num_normalized}'")
        
        try:
            if params:
                url_with_params = f"{url}?{urlencode(params)}"
                logger.info(f"Fetching cars with filters: {params}")
            else:
                url_with_params = url
                logger.info(f"Fetching all cars from {url}")
            
            response = self.client.get(url_with_params)
            response.raise_for_status()
            
            cars = response.json()

            # Проверяем формат ответа
            if not isinstance(cars, list):
                logger.error(f"Некорректный формат ответа API: {type(cars)}")
                return []
            
            logger.info(f"Received {len(cars)} cars from API")
            return cars
            
        except HTTPStatusError as e:
            logger.error(f"API returned an error: {e.response.status_code} - {e.response.text}")
            # Для 404 возвращаем пустой список (машина не найдена)
            if e.response.status_code == 404:
                return []
            raise
        except RequestError as e:
            logger.error(f"Request failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise

    def get_all_cars(self) -> List[Dict[str, Any]]:
        """
        Получает все машины без фильтров.
        """
        return self.get_cars_with_filters()

    def get_car_by_plate(self, plate: str) -> Optional[Dict[str, Any]]:
        """
        Ищет машину по госномеру.
        
        :param plate: Госномер для поиска
        :return: Информация о машине или None если не найдена
        """
        if not self.enabled:
            logger.debug(f"API 1С отключен. Не ищем машину для номера {plate}")
            return None

        try:
            normalized_plate = normalize_plate_for_api(plate)
            # Ищем по госномеру
            cars = self.get_cars_with_filters(num=normalized_plate)
            
            if not cars:
                logger.warning(f"Машина с номером {normalized_plate} не найдена в API")
                return None
            
            # Если найдено несколько машин, берем первую активную
            if len(cars) > 1:
                logger.info(f"Найдено {len(cars)} машин для номера {normalized_plate}")
                # Пытаемся найти активную машину
                active_cars = [car for car in cars if car.get("Activity", False)]
                if active_cars:
                    car = active_cars[0]
                else:
                    car = cars[0]  # Берем первую неактивную
            else:
                car = cars[0]
            
            logger.info(f"Найдена машина для номера {normalized_plate}: VIN={car.get('VIN')}")
            return car
            
        except Exception as e:
            logger.error(f"Ошибка при поиске машины по номеру {plate}: {str(e)}")
            return None

    def get_vin_by_plate(self, plate: str) -> Optional[str]:
        """
        Получает VIN по госномеру.
        
        :param plate: Госномер
        :return: VIN код или None
        """
        normalized_plate = normalize_plate_for_api(plate)
        car = self.get_car_by_plate(normalized_plate)
        if car:
            vin = car.get("VIN")
            # Проверяем, что VIN не пустой
            if vin and vin.strip():
                return vin.strip()
        return None

    def get_cars_by_plates(self, plates: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Получает информацию о нескольких машинах по госномерам.
        
        :param plates: Список госномеров
        :return: Словарь {номер: информация_о_машине}
        """
        if not self.enabled or not plates:
            return {}
        
        result = {}
        failed_plates = []
        
        # Для каждого номера делаем отдельный запрос с фильтрацией
        # (или можно получить все и фильтровать локально)
        for plate in plates:
            try:
                car = self.get_car_by_plate(plate)
                if car:
                    result[plate] = car
                else:
                    failed_plates.append(plate)
            except Exception as e:
                logger.error(f"Ошибка при поиске машины {plate}: {str(e)}")
                failed_plates.append(plate)
        
        if failed_plates:
            logger.warning(f"Не удалось найти машины для номеров: {failed_plates}")
        
        return result

    def get_car_by_vin(self, vin: str) -> Optional[Dict[str, Any]]:
        """
        Ищет машину по VIN.
        
        :param vin: VIN для поиска
        :return: Информация о машине или None
        """
        if not self.enabled:
            return None
        
        try:
            cars = self.get_cars_with_filters(vin=vin)
            if cars:
                return cars[0]
        except Exception as e:
            logger.error(f"Ошибка при поиске по VIN {vin}: {str(e)}")
        
        return None

    def get_car_extended_info(self, plate: str) -> Dict[str, Any]:
        """
        Получает расширенную информацию о машине.
        
        :param plate: Госномер
        :param inn: ИНН организации (опционально)
        :return: Словарь с расширенной информацией
        """
        car = self.get_car_by_plate(plate)
        if not car:
            return {}
        
        # Извлекаем полезные поля из ответа API
        return {
            'vin': car.get('VIN', '').strip(),
            'sts_series': car.get('STSSeries', ''),
            'sts_number': car.get('STSNumber', ''),
            'model': car.get('Model', '').strip(),
            'year': car.get('YearCar', '').split('T')[0] if car.get('YearCar') else None,
            'status': car.get('Status', ''),
            'activity': car.get('Activity', False),
        }

    def validate_plate(self, plate: str) -> bool:
        """
        Проверяет, существует ли машина с таким номером и активна ли она.
        
        :param plate: Госномер для проверки
        :return: True если машина существует и активна
        """
        normalized_plate = normalize_plate_for_api(plate)
        car = self.get_car_by_plate(normalized_plate)
        if not car:
            return False
        
        # Проверяем активность
        is_active = car.get('Activity', False)
        
        # Проверяем корректность данных
        is_correct = not car.get('IcorrectData', True)
        
        return is_active and is_correct

    def close(self):
        """Закрывает HTTP клиент"""
        if self.client:
            self.client.close()
            logger.info("API 1С Client closed")


# Синглтон экземпляр клиента
_car_api_client = None

def get_car_api_client() -> CarApiClient:
    """Фабрика для получения экземпляра API клиента"""
    global _car_api_client
    if _car_api_client is None:
        _car_api_client = CarApiClient()
    return _car_api_client


def close_car_api_client():
    """Закрывает соединение с API клиентом"""
    global _car_api_client
    if _car_api_client:
        _car_api_client.close()
        _car_api_client = None
