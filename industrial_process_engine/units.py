from __future__ import annotations

from enum import StrEnum


class MeasurementUnit(StrEnum):
    PERCENT = "%"
    RATIO = "ratio"
    COUNT = "count"
    MILLISECOND = "ms"
    SECOND = "s"
    MINUTE = "min"
    HOUR = "h"
    MILLIMETRE = "mm"
    CENTIMETRE = "cm"
    METRE = "m"
    MILLIMETRES_PER_SECOND = "mm/s"
    MILLIMETRES_PER_MINUTE = "mm/min"
    CENTIMETRES_PER_SECOND = "cm/s"
    CENTIMETRES_PER_MINUTE = "cm/min"
    METRES_PER_SECOND = "m/s"
    METRES_PER_MINUTE = "m/min"
    REVOLUTIONS_PER_MINUTE = "rpm"
    CELSIUS = "°C"
    KELVIN = "K"
    GRAM = "g"
    KILOGRAM = "kg"
    TONNE = "t"
    KILOGRAMS_PER_HOUR = "kg/h"
    TONNES_PER_HOUR = "t/h"
    NEWTON = "N"
    KILONEWTON = "kN"
    MEGANEWTON = "MN"
    NEWTONS_PER_MILLIMETRE = "N/mm"
    KILONEWTONS_PER_METRE = "kN/m"
    NEWTON_METRE = "N·m"
    KILONEWTON_METRE = "kN·m"
    PASCAL = "Pa"
    KILOPASCAL = "kPa"
    MEGAPASCAL = "MPa"
    BAR = "bar"
    AMPERE = "A"
    KILOAMPERE = "kA"
    VOLT = "V"
    KILOVOLT = "kV"
    HERTZ = "Hz"
    WATT = "W"
    KILOWATT = "kW"
    MEGAWATT = "MW"
    WATT_HOUR = "Wh"
    KILOWATT_HOUR = "kWh"
    MEGAWATT_HOUR = "MWh"
    LITRE = "L"
    CUBIC_METRE = "m³"
    LITRES_PER_MINUTE = "L/min"
    CUBIC_METRES_PER_HOUR = "m³/h"
    EURO = "€"
    EUROS_PER_HOUR = "€/h"
    EUROS_PER_TONNE = "€/t"


LINEAR_SPEED_UNITS = frozenset({
    MeasurementUnit.MILLIMETRES_PER_SECOND,
    MeasurementUnit.MILLIMETRES_PER_MINUTE,
    MeasurementUnit.CENTIMETRES_PER_SECOND,
    MeasurementUnit.CENTIMETRES_PER_MINUTE,
    MeasurementUnit.METRES_PER_SECOND,
    MeasurementUnit.METRES_PER_MINUTE,
})


_SPEED_TO_M_S: dict[MeasurementUnit, float] = {
    MeasurementUnit.METRES_PER_SECOND: 1.0,
    MeasurementUnit.METRES_PER_MINUTE: 1.0 / 60.0,
    MeasurementUnit.CENTIMETRES_PER_SECOND: 0.01,
    MeasurementUnit.CENTIMETRES_PER_MINUTE: 0.01 / 60.0,
    MeasurementUnit.MILLIMETRES_PER_SECOND: 0.001,
    MeasurementUnit.MILLIMETRES_PER_MINUTE: 0.001 / 60.0,
}


def speed_to_m_s(value: float, unit: MeasurementUnit) -> float:
    """Convert a configured linear speed to canonical metres per second."""
    return value * _SPEED_TO_M_S[unit]
