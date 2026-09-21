from industrial_process_engine.processing.consumption import ConsumptionMetricRegistry


continuous_line_consumption_metrics = ConsumptionMetricRegistry()
continuous_line_consumption_metrics.counter(
    "electricity",
    source_signal="electricity_meter_kwh",
    mass_field=None,
    cumulative_field="electricity_kwh",
    rate_field="electricity_rate_kw",
    stale_after_ms=2000,
)
continuous_line_consumption_metrics.counter(
    "natural_gas",
    source_signal="natural_gas_meter_m3",
    mass_field=None,
    cumulative_field="natural_gas_m3",
    rate_field="natural_gas_rate_m3_h",
    stale_after_ms=2000,
)


rolling_mill_consumption_metrics = ConsumptionMetricRegistry()
rolling_mill_consumption_metrics.counter(
    "electricity",
    source_signal="electricity_meter_kwh",
    mass_field=None,
    cumulative_field="electricity_kwh",
    rate_field="electricity_rate_kw",
    stale_after_ms=2000,
)


furnace_consumption_metrics = ConsumptionMetricRegistry()
furnace_consumption_metrics.counter(
    "natural_gas",
    source_signal="natural_gas_meter_m3",
    mass_field=None,
    cumulative_field="natural_gas_m3",
    rate_field="natural_gas_rate_m3_h",
    stale_after_ms=2000,
)
