from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class VehicleInfo(BaseModel):
    """Базовая информация о транспортном средстве"""
    vehicle_plate: str
    vin: Optional[str] = None
    
    # Дополнительные поля из API 1С
    car_info: Optional[Dict[str, Any]] = None
    
    class Config:
        json_encoders = {}


class OSGOPContract(BaseModel):
    """Основная модель полиса ОСГОП"""
    contract_number: str
    contract_date: Optional[str] = None
    period_from: Optional[str] = None
    period_to: Optional[str] = None
    
    insurer: Optional[str] = None
    insurer_inn: Optional[str] = None
    
    insured: Optional[str] = None
    insured_inn: Optional[str] = None
    
    premium: Optional[float] = None
    
    vehicles: List[VehicleInfo] = []
    
    # Методы для удобства
    def get_vehicle_by_plate(self, plate: str) -> Optional[VehicleInfo]:
        """Находит транспортное средство по госномеру"""
        for vehicle in self.vehicles:
            if vehicle.vehicle_plate.upper() == plate.upper():
                return vehicle
        return None
    
    def get_vehicle_by_vin(self, vin: str) -> Optional[VehicleInfo]:
        """Находит транспортное средство по VIN"""
        if not vin:
            return None
        for vehicle in self.vehicles:
            if vehicle.vin and vehicle.vin.upper() == vin.upper():
                return vehicle
        return None
    
    def has_vehicle_with_vin(self) -> bool:
        """Проверяет, есть ли хотя бы одно ТС с VIN"""
        return any(vehicle.vin for vehicle in self.vehicles)
    
    @property
    def vehicles_count(self) -> int:
        """Возвращает количество транспортных средств"""
        return len(self.vehicles)
    
    @property
    def vehicles_with_vin_count(self) -> int:
        """Возвращает количество ТС с VIN"""
        return sum(1 for vehicle in self.vehicles if vehicle.vin)
