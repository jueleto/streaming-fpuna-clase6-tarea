import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es la **resolución** del esqueleto de la cátedra: las ocho
    funciones marcadas con `TODO` están implementadas y la suite provista pasa
    completa. Cada celda conserva el contrato original y agrega, en su
    docstring, el porqué de la decisión tomada.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        from datetime import UTC

        if not isinstance(raw_value, str):
            raise TypeError(
                f"se esperaba un string ISO-8601, no {type(raw_value).__name__}"
            )
        if not raw_value.endswith("Z"):
            raise ValueError(
                f"timestamp sin zona horaria explícita: {raw_value!r}; "
                "el contrato exige ISO-8601 terminado en Z"
            )
        # fromisoformat acepta el sufijo Z recién desde 3.11; se lo saca y se
        # fija UTC a mano para que el resultado sea siempre aware.
        return datetime.fromisoformat(raw_value[:-1]).replace(tzinfo=UTC)

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        from datetime import UTC, timedelta

        if timestamp.tzinfo is None:
            raise ValueError(
                "la ventana se calcula sobre un timestamp aware; "
                "un datetime naive no define un instante"
            )
        if size_seconds <= 0:
            raise ValueError("el tamaño de ventana debe ser positivo")

        # Alineadas al epoch, no al primer evento: dos ejecuciones distintas
        # sobre los mismos datos tienen que producir los mismos límites.
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        elapsed = int((timestamp.astimezone(UTC) - epoch).total_seconds())
        start = epoch + timedelta(seconds=elapsed // size_seconds * size_seconds)
        return start, start + timedelta(seconds=size_seconds)

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.
        """
        totals: dict[tuple[str, Any, Any], int] = {}
        audit: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        for event in events:
            merchant_id = event["merchant_id"]
            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])
            window_start, window_end = assign_fixed_window(
                event_time, window_seconds
            )
            delay_seconds = int((arrival_time - event_time).total_seconds())
            key = (merchant_id, event["event_id"])

            # El estado se aísla por comercio: dos comercios pueden emitir el
            # mismo event_id sin taparse entre sí.
            duplicate = deduplicate and key in seen
            too_late = delay_seconds > allowed_lateness_seconds

            if event["status"] != "CONFIRMED":
                reason = "status"
            elif too_late:
                # El horizonte se evalúa antes que el duplicado porque en el
                # pipeline el descarte por lateness ocurre en la ventana,
                # aguas arriba del estado de deduplicación.
                reason = "too_late"
            elif duplicate:
                reason = "duplicate"
            else:
                reason = "accepted"

            accepted = reason == "accepted"
            revision = accepted and arrival_time > window_end

            if accepted:
                seen.add(key)
                bucket = (merchant_id, window_start, window_end)
                totals[bucket] = totals.get(bucket, 0) + event["amount"]

            audit.append(
                {
                    "event_id": event["event_id"],
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": revision,
                    "reason": reason,
                }
            )

        rows = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "total": total,
            }
            for (merchant_id, window_start, window_end), total in sorted(
                totals.items()
            )
        ]
        return rows, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usar Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """
        from datetime import UTC

        def con_tiempo_de_evento(event: dict[str, Any]) -> Any:
            # El timestamp del elemento es el del dominio, nunca el de llegada:
            # de eso depende en qué ventana cae un evento fuera de orden.
            return beam.window.TimestampedValue(
                event, parse_utc(event["event_time"]).timestamp()
            )

        def con_metadatos(
            item: tuple[str, int],
            window=beam.DoFn.WindowParam,
        ) -> dict[str, Any]:
            merchant_id, total = item
            # WindowParam es la única fuente confiable de los límites: el
            # elemento agregado ya no conserva ningún event_time individual.
            return {
                "merchant_id": merchant_id,
                "window_start": window.start.to_utc_datetime()
                .replace(tzinfo=UTC)
                .isoformat(),
                "window_end": window.end.to_utc_datetime()
                .replace(tzinfo=UTC)
                .isoformat(),
                "total": total,
            }

        return (
            pipeline
            | "Crear" >> beam.Create(events)
            | "TiempoDeEvento" >> beam.Map(con_tiempo_de_evento)
            | "SoloConfirmados"
            >> beam.Filter(lambda event: event["status"] == "CONFIRMED")
            | "VentanaFija"
            >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
            | "ClavePorComercio"
            >> beam.Map(lambda event: (event["merchant_id"], event["amount"]))
            | "TotalPorComercio" >> beam.CombinePerKey(sum)
            | "ConMetadatosDeVentana" >> beam.Map(con_metadatos)
        )

    return (build_windowed_totals_pipeline,)


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        # El estado vive hasta el fin de ventana más la lateness tolerada:
        # después de ese instante ningún evento de esa ventana puede llegar,
        # así que recordar sus event_id ya no sirve para nada.
        ALLOWED_LATENESS_SECONDS = 120

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            _, payment = element
            event_id = payment["event_id"]

            # El estado es local a la clave, y la clave es el comercio: por eso
            # dos comercios con el mismo event_id no se pisan.
            if event_id in set(seen_ids.read()):
                return

            seen_ids.add(event_id)
            # Se reprograma en cada elemento; el timer de watermark queda en el
            # mismo instante, así que no se acumulan disparos.
            expiry.set(window.end + self.ALLOWED_LATENESS_SECONDS)
            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            # Sin esta limpieza el set de event_id crece con cada pago y nunca
            # baja: en streaming eso es una fuga de memoria por comercio.
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """
        from apache_beam.transforms import trigger
        from apache_beam.utils.timestamp import Duration

        class DuracionEnSegundos(Duration):
            """`Duration` de Beam que además expone `seconds`.

            `Timestamp` tiene `seconds()`, pero `Duration` solo guarda
            `micros`: en apache-beam 2.74 no hay forma de leer una duración en
            segundos, y la prueba provista consulta
            `windowing.windowfn.size.seconds`. Como `Duration.of` devuelve tal
            cual cualquier instancia de `Duration`, esta subclase llega intacta
            a `Windowing` y no cambia ninguna semántica: mismos `micros`, misma
            asignación de ventanas, misma serialización al runner API.
            """

            @property
            def seconds(self) -> int:
                return self.micros // 1_000_000

        return beam.WindowInto(
            beam.window.FixedWindows(DuracionEnSegundos(window_seconds)),
            trigger=trigger.AfterWatermark(
                # Early: el operador ve movimiento sin esperar el cierre.
                early=trigger.Repeatedly(trigger.AfterProcessingTime(30)),
                # Late: cada evento tardío aceptado corrige el resultado.
                late=trigger.AfterCount(1),
            ),
            # Cada pane trae el total conocido de la ventana; el sink reemplaza
            # en lugar de sumar. Con DISCARDING el consumidor tendría que
            # acumular deltas exactamente una vez.
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=DuracionEnSegundos(allowed_lateness_seconds),
        )

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        # La clave identifica el resultado lógico, no el intento: dos panes de
        # la misma ventana y dos reintentos del mismo pane comparten clave.
        # Por eso no incluye pane_index, total ni timestamp de escritura.
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
        if attempts < 1:
            raise ValueError("se necesita al menos un intento de escritura")

        upsert_sink: dict[str, dict[str, Any]] = {}
        append_sink: list[dict[str, Any]] = []
        audit: list[dict[str, Any]] = []

        for result in results:
            idempotency_key = make_idempotency_key(result)
            for attempt in range(1, attempts + 1):
                row = {**result, "idempotency_key": idempotency_key}
                operation = "UPSERT" if idempotent else "POST"

                if idempotent:
                    # Reescribir la misma clave: el segundo intento reemplaza
                    # al primero en lugar de agregar una fila.
                    upsert_sink[idempotency_key] = row
                else:
                    append_sink.append(row)

                # La auditoría registra todos los intentos, incluso los que no
                # cambian el estado final: sin eso no se puede distinguir un
                # sink idempotente de uno que nunca fue reintentado.
                audit.append(
                    {
                        "idempotency_key": idempotency_key,
                        "attempt": attempt,
                        "operation": operation,
                        "merchant_id": result["merchant_id"],
                        "window_start": result["window_start"],
                        "total": result["total"],
                    }
                )

        materialized = (
            list(upsert_sink.values()) if idempotent else append_sink
        )
        return materialized, audit

    return (make_idempotency_key, simulate_sink_retries)


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Sobre el esqueleto fallaban con `NotImplementedError`. Con esta
    implementación las siete garantías quedan verdes:

    - [x] un duplicado no modifica el total;
    - [x] claves distintas no comparten estado;
    - [x] un evento fuera de orden cae en su ventana de evento;
    - [x] un evento con atraso aceptado produce una revisión;
    - [x] un evento demasiado tardío queda auditado;
    - [x] dos escrituras del mismo resultado dejan una sola entidad;
    - [x] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
