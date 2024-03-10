import datetime

class NormaliseIoT:

    def __init__(self, data: dict) -> None:
        self.data = data
    
    def build(self):
        switcher = {
            "location": self._build_location,
            "_build_battery_temperature": self._build_battery_temperature,
            "engine_temperature": self._build_engine_temperature,
            "speed": self._build_speed,
            "fuel_level": self._build_fuel_level,
            "tire_pressure": self._build_tire_pressure,
            "maintenance_required": self._build_maintenance_required,
            "last_service_date": self._build_last_service_date,
            "timestamp": self._build_timestamp
        }

        new_data = dict()
        for attr in switcher.keys():
            new_data.update(switcher[attr](attr))
        
        return new_data


    def _build_location(self, attr):
        """
        Method to build the location field using the longitude and latitude field
        """
        return {
            attr: f"{self.data.get('latitude')},{self.data.get('longitude')}"
        }
    
    def _build_battery_temperature(self, attr):
        """
        Method to build the battery_temperature field using the battery_temperature field
        """
        return {
            attr: self.data.get("battery_temperature", "")
        }

    def _build_engine_temperature(self, attr):
        """
        Method to build the engine_temperature field using the engine_temperature field
        """
        return {
            attr: self.data.get("engine_temperature", "")
        }
    
    def _build_speed(self, attr):
        """
        Method to build the speed field using the speed field
        """
        return {
            attr: self.data.get("speed", "")
        }
    
    def _build_fuel_level(self, attr):
        """
        Method to build the fuel_level field using the fuel_level field
        """
        return {
            attr: self.data.get("fuel_level", "")
        }
    
    def _build_tire_pressure(self, attr):
        """
        Method to build the tire_pressure field using the tire_pressure field
        """
        return {
            attr: self.data.get("tire_pressure", "")
        }
    
    def _build_maintenance_required(self, attr):
        """
        Method to build the maintenance_required field using the maintenance_required field
        """
        return {
            attr: self.data.get("maintenance_required", "")
        }
    
    def _build_last_service_date(self, attr):
        """
        Method to build the last_service_date field using the last_service_date field
        Expected input format for last_service_date: mm/dd/yyyy
        """

        new_date_value = datetime.datetime.strptime(str(self.data.get("last_service_date")), "%m/%d/%Y").date()

        return {
            attr: new_date_value
        }
    
    def _build_timestamp(self, attr):
        """
        Method to build the timestamp field using the timestamp field
        Expected input format for timestamp: epoch
        """

        new_date_value = datetime.datetime.fromtimestamp(int(self.data.get("timestamp")))

        return {
            attr: new_date_value
        }
    
class NormaliseOrders:

    def __init__(self, data: dict) -> None:
        self.data = data
    
    def build(self):
        switcher = {
            "_build_delivery_address": self._build_delivery_address,
            "delivery_status": self._build_delivery_status,
            "delivery_time": self._build_delivery_time,
            "delivery_cost": self._build_delivery_cost,
            "recipient_name": self._build_recipient_name,
            "order_date": self._build_order_date
        }

        new_data = dict()
        for attr in switcher.keys():
            new_data.update(switcher[attr](attr))
        
        return new_data
    
    def _build_delivery_address(self, attr):
        """
        Method to build the delivery_address field using the delivery_address field
        """
        return {
            attr: self.data.get("delivery_address", "")
        }

    def _build_delivery_status(self, attr):
        """
        Method to build the delivery_status field using the delivery_status field
        """
        return {
            attr: self.data.get("delivery_status", "")
        }
    
    def _build_delivery_time(self, attr):
        """
        Method to build the delivery_time field using the delivery_time field
        """
        return {
            attr: self.data.get("delivery_time", "")
        }
    
    def _build_delivery_cost(self, attr):
        """
        Method to build the delivery_cost field using the delivery_cost field
        """
        return {
            attr: self.data.get("delivery_cost", "")
        }
    
    def _build_recipient_name(self, attr):
        """
        Method to build the recipient_name field using the recipient_name field
        """
        return {
            attr: self.data.get("recipient_name", "")
        }

    def _build_order_date(self, attr):
        """
        Method to build the order_date field using the order_date field
        Expected input format for order_date: mm/dd/yyyy
        """

        new_date_value = datetime.datetime.strptime(str(self.data.get("order_date")), "%m/%d/%Y").date()

        return {
            attr: new_date_value
        }
