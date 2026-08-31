# Tarea 3 — Beam avanzado

**Estudiante:** Micaela Sánchez  
**Módulo:** Streaming de datos y sus aplicaciones

Implementación de la Tarea 3 correspondiente al módulo **Streaming de datos y sus aplicaciones**.

El proyecto desarrolla un pipeline de procesamiento de pagos con Apache Beam, considerando tiempo de evento, ventanas fijas, eventos tardíos, deduplicación con estado, timers, triggers y escritura idempotente.

## Objetivo

El pipeline produce totales de pagos confirmados por comercio y por minuto, considerando las siguientes condiciones:

- uso de `event_time` para asignar cada evento a su ventana;
- ventanas fijas de 60 segundos;
- tolerancia de hasta 120 segundos para eventos tardíos;
- procesamiento exclusivo de eventos con estado `CONFIRMED`;
- deduplicación de `event_id` dentro de cada comercio;
- conservación de los límites temporales de cada ventana;
- uso de estado y timers para controlar la deduplicación;
- política de triggers con resultados tempranos, on-time y tardíos;
- materialización de resultados mediante un identificador idempotente.

## Decisiones de implementación

### Tiempo de evento y ventanas

Los timestamps se convierten a objetos `datetime` con información de zona horaria. La asignación de ventanas se realiza a partir de `event_time` y no de `arrival_time`.

Se utilizan ventanas fijas de 60 segundos con intervalos de la forma `[inicio, fin)`. De esta manera, un evento ocurrido a las `13:00:42` pertenece a la ventana comprendida entre las `13:00:00` y las `13:01:00`.

### Eventos tardíos

La tolerancia predeterminada es de 120 segundos. El atraso se obtiene mediante la diferencia entre `arrival_time` y `event_time`.

Un evento confirmado que se encuentra dentro de la tolerancia puede incorporarse al resultado. Cuando llega después del cierre de su ventana se identifica como una revisión. Los eventos que superan la tolerancia son registrados en la auditoría, pero no modifican los totales.

### Deduplicación y estado

La deduplicación utiliza `event_id` y mantiene el estado separado por comercio. Esto permite que dos comercios diferentes puedan utilizar el mismo identificador de evento sin interferir entre sí.

La implementación con Apache Beam utiliza `SetStateSpec` para conservar temporalmente los identificadores ya procesados. Se configura un timer de tiempo de evento para eliminar el estado una vez finalizada la ventana y transcurrida la tolerancia establecida.

Esta expiración evita conservar identificadores indefinidamente y limita el crecimiento del estado.

### Triggers y panes

La política de ventanas utiliza un trigger basado en watermark. Se incluye una estimación temprana mediante processing time y revisiones tardías cuando llegan nuevos elementos.

El modo de acumulación utilizado es `ACCUMULATING`, por lo que cada pane posterior conserva los resultados acumulados de panes anteriores. Se establece una tolerancia de 120 segundos para la recepción de eventos tardíos.

### Idempotencia y reintentos

Para representar una escritura idempotente se construye un identificador a partir de:

`merchant_id|window_start`

En modo idempotente, los reintentos realizan operaciones `UPSERT`, por lo que varios intentos asociados al mismo resultado lógico convergen en una única entidad materializada.

También se simula un sink append-only mediante operaciones `POST`. En este caso, cada reintento genera una nueva fila, lo que permite observar la diferencia entre una escritura idempotente y una escritura que materializa todos los intentos.

## Trade-offs

La tolerancia de eventos tardíos permite incorporar información que llega después del cierre inicial de una ventana, pero implica mantener estado durante más tiempo.

El modo `ACCUMULATING` facilita la emisión de revisiones completas, aunque puede incrementar el volumen de información procesada respecto de una estrategia que emita únicamente los cambios.

La escritura mediante `UPSERT` evita duplicados provocados por reintentos. Un sink append-only conserva todos los intentos, pero requiere mecanismos adicionales si se desea evitar duplicación en el sistema de destino.

## Ejecutar con Docker

Desde el directorio del proyecto:

```bash
docker compose up --build notebook
```

Luego abrir `http://localhost:2718`.

Docker inicia Marimo en modo editor. Los cambios realizados en `notebook.py` se guardan en el directorio local.

El editor utiliza `--no-token` para simplificar el trabajo en `localhost`, por lo que no debe exponerse directamente a una red pública.

## Ejecutar con uv

Instalar las dependencias:

```bash
uv sync --frozen
```

Abrir el notebook con Marimo:

```bash
uv run marimo edit notebook.py
```

## Pruebas

La suite provista se ejecuta mediante:

```bash
uv run pytest -q
```

Resultado obtenido:

```text
13 passed in 1.83s
```

Las pruebas verifican el comportamiento ante:

- eventos fuera de orden;
- eventos duplicados;
- aislamiento del estado por comercio;
- eventos tardíos dentro y fuera de la tolerancia;
- asignación mediante tiempo de evento;
- agregaciones por ventana;
- expiración del estado mediante timer;
- política de triggers;
- reintentos sobre sinks idempotentes y append-only.

También se realizaron las validaciones de estilo y estructura:

```bash
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

Resultado de Ruff:

```text
All checks passed!
```

La ejecución de `marimo check --strict notebook.py` finalizó sin errores.

## Estructura del proyecto

```text
data/
    payments.jsonl
tests/
    conftest.py
    test_assignment.py
notebook.py
README.md
pyproject.toml
uv.lock
Dockerfile
docker-compose.yml
Makefile
```

El archivo `data/payments.jsonl` se mantiene sin modificaciones.

## Reproducibilidad

Para reproducir la solución desde una copia limpia del repositorio:

```bash
uv sync --frozen
uv run pytest -q
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

Estas instrucciones permiten instalar el entorno definido por el proyecto y ejecutar las pruebas y validaciones utilizadas para comprobar la implementación.
