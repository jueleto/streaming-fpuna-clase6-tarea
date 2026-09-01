"""Corre el pipeline sobre `data/payments.jsonl` y deja la evidencia por stdout.

    uv run python ejecutar.py > evidencia/salida-pipeline.txt

Carga las funciones desde `notebook.py` con la misma técnica que
`tests/conftest.py` —compilar solo las definiciones de la tarea, sin arrancar
la aplicación Marimo— para que la evidencia salga del mismo código que las
pruebas ejercitan, y no de una copia.
"""

from __future__ import annotations

import ast
import json
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer

BASE_DIR = Path(__file__).parent
NOTEBOOK_PATH = BASE_DIR / "notebook.py"
DATA_PATH = BASE_DIR / "data" / "payments.jsonl"

DEFINICIONES = {
    "parse_utc",
    "assign_fixed_window",
    "summarize_payments",
    "build_windowed_totals_pipeline",
    "DeduplicatePayments",
    "build_trigger_policy",
    "make_idempotency_key",
    "simulate_sink_retries",
}


def cargar_solucion() -> SimpleNamespace:
    tree = ast.parse(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    cuerpo: list[ast.stmt] = []
    for celda in tree.body:
        if not isinstance(celda, ast.FunctionDef) or celda.name != "_":
            continue
        for sentencia in celda.body:
            if (
                isinstance(sentencia, ast.FunctionDef | ast.ClassDef)
                and sentencia.name in DEFINICIONES
            ):
                cuerpo.append(sentencia)

    espacio: dict[str, Any] = {
        "__name__": "assignment_notebook",
        "Any": Any,
        "Iterable": Iterable,
        "SetStateSpec": SetStateSpec,
        "StrUtf8Coder": StrUtf8Coder,
        "TimeDomain": TimeDomain,
        "TimerSpec": TimerSpec,
        "beam": beam,
        "datetime": datetime,
        "on_timer": on_timer,
    }
    modulo = ast.Module(body=cuerpo, type_ignores=[])
    ast.fix_missing_locations(modulo)
    exec(compile(modulo, str(NOTEBOOK_PATH), "exec"), espacio)
    return SimpleNamespace(**{nombre: espacio[nombre] for nombre in DEFINICIONES})


def cargar_eventos() -> list[dict[str, Any]]:
    return [
        json.loads(linea)
        for linea in DATA_PATH.read_text(encoding="utf-8").splitlines()
        if linea.strip()
    ]


def titulo(texto: str) -> None:
    print(f"\n{texto}\n{'=' * len(texto)}")


def gs(monto: int) -> str:
    return f"{monto:>9,}".replace(",", ".")


def main() -> None:
    solucion = cargar_solucion()
    eventos = cargar_eventos()

    titulo("Entrada")
    print(f"{len(eventos)} eventos en {DATA_PATH.relative_to(BASE_DIR)}")

    totals, audit = solucion.summarize_payments(eventos)

    titulo("Auditoría evento por evento")
    print(
        f"{'event_id':<9} {'comercio':<9} {'atraso':>7} "
        f"{'dup':>5} {'tarde':>6} {'rev':>5} {'acepta':>7}  motivo"
    )
    for fila in audit:
        print(
            f"{fila['event_id']:<9} {fila['merchant_id']:<9} "
            f"{fila['delay_seconds']:>6}s "
            f"{str(fila['duplicate']):>5} {str(fila['too_late']):>6} "
            f"{str(fila['revision']):>5} {str(fila['accepted']):>7}  "
            f"{fila['reason']}"
        )
    aceptados = sum(1 for fila in audit if fila["accepted"])
    print(f"\n{aceptados} aceptados de {len(audit)}; {len(totals)} totales producidos")

    titulo("Totales por comercio y ventana (oráculo determinista)")
    for fila in totals:
        print(
            f"{fila['merchant_id']:<9} {fila['window_start']} → "
            f"{fila['window_end']}  {gs(fila['total'])}"
        )

    titulo("Los mismos totales calculados por el pipeline Beam")
    with beam.Pipeline() as pipeline:
        (
            solucion.build_windowed_totals_pipeline(pipeline, eventos)
            | "Imprimir"
            >> beam.Map(
                lambda fila: print(
                    f"{fila['merchant_id']:<9} {fila['window_start']} → "
                    f"{fila['window_end']}  {gs(fila['total'])}"
                )
            )
        )

    titulo("Pipeline compuesto: ventana + horizonte + estado")
    print(
        "build_windowed_totals_pipeline no deduplica ni descarta tardíos: ése\n"
        "es su contrato. Componiéndolo con el filtro de horizonte y con\n"
        "DeduplicatePayments, el pipeline converge a los totales del oráculo.\n"
    )
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | "Crear" >> beam.Create(eventos)
            | "TiempoDeEvento"
            >> beam.Map(
                lambda evento: beam.window.TimestampedValue(
                    evento, solucion.parse_utc(evento["event_time"]).timestamp()
                )
            )
            | "SoloConfirmados"
            >> beam.Filter(lambda evento: evento["status"] == "CONFIRMED")
            | "DentroDelHorizonte"
            >> beam.Filter(
                lambda evento: (
                    solucion.parse_utc(evento["arrival_time"])
                    - solucion.parse_utc(evento["event_time"])
                ).total_seconds()
                <= 120
            )
            | "PoliticaDeVentana" >> solucion.build_trigger_policy()
            | "ClavePorComercio"
            >> beam.Map(lambda evento: (evento["merchant_id"], evento))
            | "Deduplicar" >> beam.ParDo(solucion.DeduplicatePayments())
            | "SoloMonto"
            >> beam.Map(lambda item: (item[0], item[1]["amount"]))
            | "TotalPorComercio" >> beam.CombinePerKey(sum)
            | "ConMetadatos"
            >> beam.Map(
                lambda item,
                ventana=beam.DoFn.WindowParam,
                pane=beam.DoFn.PaneInfoParam: print(
                    f"{item[0]:<9} {ventana.start.to_utc_datetime().isoformat()}"
                    f"+00:00 → {ventana.end.to_utc_datetime().isoformat()}+00:00"
                    f"  {gs(item[1])}  pane #{pane.index} "
                    f"{'FINAL' if pane.is_last else 'parcial'}"
                )
            )
        )

    titulo("Política de ventana y triggers")
    politica = solucion.build_trigger_policy()
    ventana = politica.windowing
    print(f"ventana fija            : {ventana.windowfn.size.seconds} s")
    print(f"lateness permitida      : {ventana.allowed_lateness.seconds} s")
    print(f"trigger                 : {ventana.triggerfn}")
    print(f"modo de acumulación     : {ventana.accumulation_mode} (2 = ACCUMULATING)")

    titulo("Sink: mismos resultados, dos contratos de escritura")
    for idempotente in (True, False):
        materializados, intentos = solucion.simulate_sink_retries(
            totals, attempts=2, idempotent=idempotente
        )
        modo = "UPSERT idempotente" if idempotente else "POST append-only"
        print(
            f"{modo:<20}: {len(intentos)} intentos → "
            f"{len(materializados)} filas materializadas"
        )

    print("\nclaves idempotentes:")
    for fila in totals:
        print(f"  {solucion.make_idempotency_key(fila)}")


if __name__ == "__main__":
    main()
