from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

import os

from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter, AzureMonitorMetricExporter
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

console = Console()


@dataclass
class Monitor:
    log_path: Path
    metrics_path: Path
    azure_connection_string: str | None = None
    service_name: str = "comboi"
    _metrics: Dict[str, Any] = field(default_factory=dict)
    _observables: Dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.metrics_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_azure_monitor()

    def log(self, message: str) -> None:
        console.log(message)
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(f"{time.asctime()} {message}\n")
        if self._tracer:
            with self._tracer.start_as_current_span("monitor.log") as span:
                span.set_attribute("message", message)
                span.set_attribute("service.name", self.service_name)

    def record_metric(self, key: str, value: Any) -> None:
        self._metrics[key] = value
        with self.metrics_path.open("w", encoding="utf-8") as fh:
            json.dump(self._metrics, fh, indent=2)
        if self._meter:
            observable_name = f"comboi.{key}"
            if observable_name not in self._observables:
                gauge = self._meter.create_observable_gauge(
                    observable_name,
                    callbacks=[lambda _: [(self._metrics[key], {})]],
                    description="Pipeline metric",
                )
                self._observables[observable_name] = gauge

    def progress(self) -> Progress:
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console,
        )

    def _init_azure_monitor(self) -> None:
        self._meter = None
        self._tracer = None
        connection_string = self.azure_connection_string or os.getenv("AZURE_MONITOR_CONNECTION_STRING")
        if not connection_string:
            return
        resource = Resource(attributes={SERVICE_NAME: self.service_name})
        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(AzureMonitorLogExporter(connection_string=connection_string))
        )
        trace.set_tracer_provider(tracer_provider)
        self._tracer = trace.get_tracer(__name__)

        metric_reader = PeriodicExportingMetricReader(
            AzureMonitorMetricExporter(connection_string=connection_string),
            export_interval_millis=60000,
        )
        meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        metrics.set_meter_provider(meter_provider)
        self._meter = metrics.get_meter(__name__)

