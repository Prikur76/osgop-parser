from pydantic import BaseModel, ConfigDict, Field
from typing import List, Optional, Dict, Any


class VehicleInfo(BaseModel):
    """Информация о транспортном средстве"""
    vehicle_plate_cyr: str = Field(..., description="Госномер в кириллице")
    vehicle_plate_lat: str = Field(..., description="Госномер в латинице (нормализованный)")
    vin: str | None = Field(None, description="VIN номер")
    car_info: Optional[Dict[str, Any]] = Field(None, description="Информация из API: марка, модель, год")


class OSGOPContract(BaseModel):
    """Модель полиса ОСГОП"""
    contract_number: str = Field(..., description="Номер полиса ROSX...")
    contract_date: str | None = Field(None, description="Дата заключения договора")
    period_from: str | None = Field(None, description="Начало действия")
    period_to: str | None = Field(None, description="Окончание действия")
    insurer: str | None = Field(None, description="Страховщик")
    insurer_inn: str | None = Field(None, description="ИНН страховщика")
    insured: str | None = Field(None, description="Страхователь")
    insured_inn: str | None = Field(None, description="ИНН страхователя")
    bonus: float | None = Field(None, description="Страховая премия")
    vehicles: List[VehicleInfo] = Field(default_factory=list, description="Список ТС")
    
    model_config = ConfigDict(
        json_encoders = {
            float: lambda v: round(v, 2) if v else None
        },
    )
    
    # Методы для удобства
    def get_vehicle_by_plate(self, plate: str) -> Optional[VehicleInfo]:
        """Находит транспортное средство по госномеру"""
        for vehicle in self.vehicles:
            if vehicle.vehicle_plate_cyr.upper() == plate.upper():
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
