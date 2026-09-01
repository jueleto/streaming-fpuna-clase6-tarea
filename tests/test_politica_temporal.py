"""Pruebas propias: panes late con TestStream, expiración del estado y ventanas.

`test_assignment.py` es la suite provista por la cátedra y no se modifica. Este
archivo agrega los casos que el enunciado del notebook pide además de ella:

- una prueba temporal con `TestStream` que evidencie un resultado late aceptado;
- el contraejemplo: un evento fuera del horizonte que no corrige el total;
- la deduplicación dentro de una misma clave, dentro de un pipeline real;
- el instante exacto en que queda programado el timer de expiración;
- que la política de triggers asigne exactamente las mismas ventanas que
  `FixedWindows` estándar (ver `DuracionEnSegundos` en el notebook).
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import IntervalWindow, TimestampedValue, WindowFn
from apache_beam.utils.timestamp import Timestamp

# 2026-07-24T13:00:00Z, el mismo instante que usa data/payments.jsonl.
BASE = 1784898000


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def test_pane_late_dentro_de_la_tolerancia_corrige_el_total(solution):
    """Un pago que llega después del watermark revisa el total de su ventana.

    La ventana [13:00, 13:01) emite un pane on-time de 120.000 y, cuando llega
    el pago de 13:00:42, un pane late de 170.000. Con acumulación ACCUMULATING
    el segundo reemplaza al primero, no se suma a él.
    """
    stream = (
        BeamTestStream()
        .advance_watermark_to(BASE)
        .add_elements([TimestampedValue(("m-azul", 120_000), BASE + 5)])
        # El watermark pasa el fin de ventana: se emite el pane on-time.
        .advance_watermark_to(BASE + 61)
        # Este pago ocurrió a las 13:00:42 pero se observa ahora: es late,
        # y está dentro de los 120 s de tolerancia.
        .add_elements([TimestampedValue(("m-azul", 50_000), BASE + 42)])
        .advance_watermark_to_infinity()
    )

    with _streaming_pipeline() as pipeline:
        totales = (
            pipeline
            | stream
            | solution.build_trigger_policy()
            | beam.CombinePerKey(sum)
        )
        assert_that(
            totales,
            equal_to([("m-azul", 120_000), ("m-azul", 170_000)]),
        )


def test_evento_fuera_del_horizonte_no_corrige_el_total(solution):
    """Pasada la lateness, el mismo evento tardío ya no cambia el resultado."""
    stream = (
        BeamTestStream()
        .advance_watermark_to(BASE)
        .add_elements([TimestampedValue(("m-verde", 80_000), BASE + 18)])
        .advance_watermark_to(BASE + 61)
        # 13:03:20 > fin de ventana (13:01:00) + 120 s: el estado ya se liberó.
        .advance_watermark_to(BASE + 200)
        .add_elements([TimestampedValue(("m-verde", 30_000), BASE + 51)])
        .advance_watermark_to_infinity()
    )

    with _streaming_pipeline() as pipeline:
        totales = (
            pipeline
            | stream
            | solution.build_trigger_policy()
            | beam.CombinePerKey(sum)
        )
        # Solo el pane on-time: los 30.000 no entran a ningún total.
        assert_that(totales, equal_to([("m-verde", 80_000)]))


def test_duplicado_se_emite_una_sola_vez_dentro_de_la_clave(solution):
    """El estado por clave absorbe el event_id repetido del mismo comercio."""
    eventos = [
        ("m-verde", {"event_id": "p-002", "amount": 80_000}),
        ("m-verde", {"event_id": "p-002", "amount": 80_000}),
        ("m-verde", {"event_id": "p-005", "amount": 90_000}),
    ]

    with BeamTestPipeline() as pipeline:
        salida = (
            pipeline
            | beam.Create(eventos)
            | beam.WindowInto(beam.window.FixedWindows(60))
            | beam.ParDo(solution.DeduplicatePayments())
            | beam.Map(lambda item: item[1]["event_id"])
        )
        assert_that(salida, equal_to(["p-002", "p-005"]))


def test_process_programa_el_timer_al_final_de_la_ventana_mas_lateness(solution):
    """El timer se arma en fin de ventana + lateness, no en el fin de ventana.

    Si se armara en el fin de ventana, el estado se limpiaría justo antes del
    período en que todavía se aceptan revisiones y un duplicado tardío volvería
    a sumar.
    """

    class FakeSet:
        def __init__(self):
            self.items: set[str] = set()

        def read(self):
            return list(self.items)

        def add(self, value):
            self.items.add(value)

    class FakeTimer:
        def __init__(self):
            self.when = None

        def set(self, moment):
            self.when = moment

    seen_ids, expiry = FakeSet(), FakeTimer()
    window = IntervalWindow(Timestamp(BASE), Timestamp(BASE + 60))

    emitidos = list(
        solution.DeduplicatePayments().process(
            ("m-azul", {"event_id": "p-001"}),
            seen_ids=seen_ids,
            window=window,
            expiry=expiry,
        )
    )

    assert len(emitidos) == 1
    assert seen_ids.items == {"p-001"}
    assert expiry.when == Timestamp(BASE + 60 + 120)

    # La segunda aparición no se emite y no vuelve a tocar el estado.
    repetidos = list(
        solution.DeduplicatePayments().process(
            ("m-azul", {"event_id": "p-001"}),
            seen_ids=seen_ids,
            window=window,
            expiry=expiry,
        )
    )
    assert repetidos == []


def test_la_politica_asigna_las_mismas_ventanas_que_fixed_windows(solution):
    """`DuracionEnSegundos` no cambia la semántica de la ventana.

    El notebook envuelve el tamaño en una subclase de `Duration` porque la
    prueba provista consulta `size.seconds`, atributo que apache-beam 2.74 no
    expone. Esta prueba fija que el envoltorio es inocuo: mismas ventanas y
    mismos micros que `FixedWindows(60)`.
    """
    politica = solution.build_trigger_policy()
    estandar = beam.window.FixedWindows(60)

    assert politica.windowing.windowfn.size.micros == estandar.size.micros
    for offset in (0, 5, 42, 59, 60, 61, 3_600):
        contexto = WindowFn.AssignContext(Timestamp(BASE + offset))
        assert politica.windowing.windowfn.assign(contexto) == estandar.assign(
            contexto
        )
