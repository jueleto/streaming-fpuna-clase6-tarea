# Tarea 3 — Estado, duplicados e idempotencia con Apache Beam

Pipeline de pagos con tiempo de evento, ventanas, estado por clave y salida
idempotente. Implementación de `notebook.py` sobre el esqueleto de la cátedra
(`github.com/rparrapy/streaming-fpuna-clase6-tarea`, commit
`2146eeb9880cf53a949a743b3496cc69d3cb0fd9`).

**Caso:** un adquirente calcula, por comercio y por minuto, el total de pagos
confirmados. Los pagos llegan desordenados y a veces repetidos —el switch
reintenta ante timeouts— y el destino se reprocesa, así que el resultado tiene
que ser el mismo se corra una vez o cinco.

**Autor:** Juan Ledesma

**Estado: 18 pruebas verdes** — las 13 provistas más 5 propias — con `ruff` y
`marimo check --strict` limpios.

```
tests/test_assignment.py ......... 13 passed   (suite provista, sin modificar)
tests/test_politica_temporal.py ... 5 passed   (TestStream, timer, ventanas)
```

Evidencia completa en [`evidencia/pytest.txt`](./evidencia/pytest.txt) y
[`evidencia/salida-pipeline.txt`](./evidencia/salida-pipeline.txt).

---

## Cómo correrlo

### Con `uv` (ruta recomendada)

```bash
uv sync --frozen                  # apache-beam 2.74 exige Python >=3.12,<3.13
uv run pytest                     # 18 pruebas
uv run python ejecutar.py         # pipeline sobre data/payments.jsonl
uv run ruff check .
uv run marimo check --strict notebook.py
uv run marimo edit notebook.py    # editor interactivo en localhost:2718
```

`make install | test | check | edit` envuelve los mismos comandos.

> `apache-beam==2.74.0` requiere Python `>=3.12,<3.13`, y el `python3` de la
> mayoría de los sistemas ya es 3.13. `uv` provisiona el intérprete correcto por
> proyecto sin tocar el sistema; por eso no se usa `pip` ni un venv a mano.

### Con Docker

```bash
docker compose up --build notebook          # Marimo en http://localhost:2718
docker compose exec notebook uv run pytest
```

El editor arranca con `--no-token` para simplificar el trabajo en `localhost`;
no debe exponerse a una red pública.

---

## El contrato, y por qué cada pieza es necesaria

| Decisión | Valor | Qué rompe si se cambia |
|---|---|---|
| Reloj | `event_time` | Con `arrival_time`, un replay del mismo día da otro resultado. |
| Ventana | fija de 60 s por `merchant_id` | Es el grano de la conciliación y la clave del estado. |
| Filtro | solo `status == "CONFIRMED"` | `PENDING` y `REJECTED` no son plata cobrada. |
| Lateness | 120 s | Menos pierde revisiones legítimas; más sostiene estado sin necesidad. |
| Panes | acumulativos | Con deltas, el consumidor tiene que sumar exactamente una vez. |
| Estado | dedup por `event_id`, aislado por comercio, expirado por timer | Sin aislar, dos comercios se pisan; sin expirar, hay fuga de memoria. |
| Sink | UPSERT con clave `merchant_id\|window_start` | Con append-only, cada reintento y cada pane duplican la fila. |

Las siete decisiones son una sola semántica: **la ventana define la clave del
estado, y esa clave es la clave del sink**. Cambiar una obliga a revisar las
otras.

---

## Decisiones de implementación

### 1. `parse_utc` — timestamps aware o error

Rechaza cualquier string que no termine en `Z` en lugar de asumir UTC. Un
timestamp naive no identifica un instante, y en un pipeline temporal esa
ambigüedad no aparece como excepción sino como un total mal calculado tres
ventanas después.

### 2. `assign_fixed_window` — alineada al epoch

Los límites se calculan contra el epoch Unix, no contra el primer evento del
lote. Es la diferencia entre una ventana reproducible y una que depende de qué
datos entraron primero: dos ejecuciones sobre el mismo conjunto tienen que
producir exactamente los mismos límites, condición sin la cual un
reprocesamiento no sirve para comparar contra la corrida original.

### 3. `summarize_payments` — el oráculo determinista

Versión pura en Python que sirve de referencia del pipeline. El orden de las
decisiones es deliberado:

```
status  →  horizonte (too_late)  →  duplicado  →  aceptado
```

El horizonte se evalúa **antes** que el duplicado porque en el pipeline el
descarte por lateness ocurre en la ventana, aguas arriba del estado de
deduplicación: un evento que la ventana ya descartó nunca llega al `DoFn` con
estado. Invertir el orden daría un veredicto que el pipeline no puede reproducir.

Un evento aceptado que llega después del cierre de su ventana se marca
`revision=True`: no es un error, es una corrección. Sobre el dataset provisto,
5 de 9 eventos se aceptan y producen 4 totales.

### 4. `build_windowed_totals_pipeline` — el contrato mínimo, y lo que no hace

Create → `TimestampedValue` con `event_time` → filtro `CONFIRMED` →
`FixedWindows` → clave por comercio → `CombinePerKey` → fila con `WindowParam`.

**No deduplica ni descarta tardíos**, y eso es visible en la evidencia: sobre
`payments.jsonl` da ₲ 190.000 para `m-verde` en el minuto 13:00, donde el
oráculo da ₲ 80.000. La diferencia son exactamente el duplicado `p-002`
(₲ 80.000) y el evento fuera de horizonte `p-007` (₲ 30.000).

No es un defecto: es el contrato de esta función, y la brecha muestra para qué
sirven las piezas siguientes. `ejecutar.py` corre además el **pipeline
compuesto** —horizonte + `DeduplicatePayments` + política de triggers— y ahí sí
converge a los números del oráculo.

Los límites de ventana salen de `WindowParam` y no del elemento: después de
`CombinePerKey` el agregado ya no conserva ningún `event_time` individual del
cual derivarlos.

### 5. `DeduplicatePayments` — estado por clave y timer

```python
if event_id in set(seen_ids.read()):
    return
seen_ids.add(event_id)
expiry.set(window.end + self.ALLOWED_LATENESS_SECONDS)
yield element
```

Tres decisiones en cinco líneas:

- **El estado es local a la clave**, y la clave es el comercio. Por eso dos
  comercios pueden emitir el mismo `event_id` sin taparse: es lo que fija
  `test_deduplication_is_isolated_by_merchant`. Un set global exigiría
  coordinación entre workers y volvería el pipeline no paralelizable.
- **El timer se arma en `fin de ventana + lateness`, no en el fin de ventana.**
  Si expirara al cerrar la ventana, el estado se limpiaría justo antes del
  período en que todavía se aceptan revisiones, y un duplicado tardío volvería a
  sumar. `test_process_programa_el_timer_al_final_de_la_ventana_mas_lateness`
  fija el instante exacto.
- **La expiración no es opcional.** Sin `expire`, el set de `event_id` crece con
  cada pago y nunca baja: en un flujo no acotado eso es una fuga de memoria por
  comercio, y con 3.000 comercios activos el pipeline se cae solo. El timer de
  watermark es lo que hace que el estado sea proporcional a la ventana viva y no
  a la historia completa.

### 6. `build_trigger_policy` — early, on-time y late

`AfterWatermark(early=Repeatedly(AfterProcessingTime(30)), late=AfterCount(1))`,
`allowed_lateness=120`, `ACCUMULATING`.

El pane on-time **no es final**: es la mejor estimación disponible cuando el
watermark pasa el fin de la ventana. `test_pane_late_dentro_de_la_tolerancia_corrige_el_total`
lo demuestra con `TestStream`: la ventana emite 120.000 on-time y 170.000 cuando
llega el pago tardío. El contraejemplo
(`test_evento_fuera_del_horizonte_no_corrige_el_total`) muestra que pasada la
lateness el mismo evento ya no cambia nada.

Se eligió `ACCUMULATING` y no `DISCARDING` porque el destino hace UPSERT: cada
pane trae el total conocido y **reemplaza** al anterior. Con deltas, el
consumidor tendría que sumarlos de forma durable y exactamente una vez, lo que
mueve el problema de la exactitud aguas abajo, justo donde no se lo quiere.

> **Nota sobre `DuracionEnSegundos`.** La prueba provista consulta
> `policy.windowing.windowfn.size.seconds`, pero en apache-beam 2.74
> `Duration` solo guarda `micros` y no expone `seconds` (sí lo hace
> `Timestamp`, como método). No hay configuración estándar que satisfaga esa
> aserción. La solución es una subclase de `Duration` que agrega la propiedad
> faltante: como `Duration.of` devuelve intacta cualquier instancia de
> `Duration`, llega tal cual a `Windowing`. No cambia ninguna semántica —mismos
> `micros`, misma asignación de ventanas—, y
> `test_la_politica_asigna_las_mismas_ventanas_que_fixed_windows` lo verifica
> comparando contra `FixedWindows(60)` estándar. Se dejó constancia acá porque
> es un parche sobre una API incompleta, no una decisión de diseño.

### 7 y 8. `make_idempotency_key` y `simulate_sink_retries`

La clave identifica el **resultado lógico**, no el intento ni la versión: dos
panes de la misma ventana y dos reintentos del mismo pane comparten
`merchant_id|window_start`. Por eso no incluye `pane_index`, `total` ni
timestamp de escritura — cualquiera de los tres reintroduciría el duplicado que
la clave existe para evitar.

La evidencia lo cierra: **8 intentos → 4 filas** con UPSERT, **8 intentos → 8
filas** con append-only. Y el pipeline compuesto emite 8 panes para 4 ventanas
lógicas (pane #0 on-time y pane #1 final), de modo que el sink absorbe tanto los
reintentos por fallas como las revisiones legítimas con el mismo mecanismo.

La auditoría registra **todos** los intentos, incluso los que no cambian el
estado final: sin eso no hay forma de distinguir un sink idempotente que
funcionó de uno que nunca fue reintentado.

---

## Pruebas

`tests/test_assignment.py` es la suite de la cátedra y **no se modificó**.
`tests/test_politica_temporal.py` agrega los casos que el enunciado del notebook
pide además de ella:

| Prueba | Qué fija |
|---|---|
| `test_pane_late_dentro_de_la_tolerancia_corrige_el_total` | con `TestStream`: pane on-time 120.000 → pane late 170.000 |
| `test_evento_fuera_del_horizonte_no_corrige_el_total` | pasada la lateness, el evento tardío no suma |
| `test_duplicado_se_emite_una_sola_vez_dentro_de_la_clave` | dedup dentro de un pipeline real, no solo en el oráculo |
| `test_process_programa_el_timer_al_final_de_la_ventana_mas_lateness` | el timer queda en fin + 120 s, y la segunda aparición no emite |
| `test_la_politica_asigna_las_mismas_ventanas_que_fixed_windows` | `DuracionEnSegundos` no altera la semántica |

`data/payments.jsonl` no se modificó: es el dataset provisto por la cátedra, con
sus 9 eventos —duplicado, `PENDING`, desorden y un evento fuera del horizonte—.
Las pruebas propias construyen sus casos con `TestStream` en vez de tocarlo.

---

## Trade-offs asumidos

| Se eligió | En vez de | Por qué |
|---|---|---|
| `ACCUMULATING` | `DISCARDING` | El sink hace UPSERT; los deltas exigirían exactly-once en el consumidor. |
| Timer de watermark | Timer de processing time | El estado debe expirar según el tiempo del dominio, no según cuánto tardó el worker. |
| Dedup por `event_id` en estado | Dedup en el sink | En el sink llega tarde: el duplicado ya contaminó el agregado. |
| Lateness de 120 s | Horizonte infinito | Completitud máxima teórica a cambio de estado no acotado y un contrato que nunca cierra. |
| Filtro `CONFIRMED` explícito | Sumar todo y restar después | Restar exige recordar qué se sumó; filtrar es idempotente por construcción. |

Lo que este pipeline **no** resuelve: el watermark es del runner, así que la
política de lateness es tan buena como esa estimación —una heurística sobre el
avance del flujo, no una garantía—; y la idempotencia del sink es simulada en
memoria: contra una base real haría falta además una transacción o un
`INSERT ... ON CONFLICT`.

---

> **Nota sobre el uso de IA generativa:** parte del código, la infraestructura y
> la documentación de este trabajo fue desarrollada con asistencia de
> herramientas de IA generativa. Cada fragmento generado fue validado, probado y
> corregido por el autor.
