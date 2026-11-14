# Desempeño de brokers ante ráfagas de tráfico

**Autor:** Martinez Pazmiño Fredy David  
**Trabajo:** Investigación Formativa Individual, Unidad 2  
**Documento:** [Artículo IEEE Conference](articulo.pdf) · [Fuente Quarto](articulo.qmd) · [Bitácora de IA](PROMPTS_OPENCODE.md)

## Resumen

Comparamos RabbitMQ 3.12.1 (AMQP 0-9-1) con el emulador oficial de Google Cloud Pub/Sub 0.8.38 (gRPC). Ocho bloques de cuatro ensayos produjeron 64 000 registros de publicación y recepción. Cada condición ofreció 2 000 mensajes de 1 KiB en dos segundos. El perfil uniforme programó 1 000 mensajes/s y el de ráfaga concentró el mismo volumen en los primeros 250 ms de cada segundo.

| Configuración | p99 uniforme | p99 en ráfaga | Ausentes a 0,5 s (uniforme / ráfaga) |
| --- | ---: | ---: | ---: |
| RabbitMQ | 639,81 ms | 1 714,13 ms | 940 / 3 031 |
| Emulador Pub/Sub | 3,00 ms | 267,61 ms | 0 / 0 |

Cada condición acumula 16 000 mensajes. Se confirmaron los 64 000 y se observó su recepción antes del cierre a cinco segundos. Los valores de p99 son medianas de ocho percentiles por ensayo. La comparación describe dos configuraciones locales con semánticas diferentes de persistencia.

## Ficha técnica

| Componente | Entorno de la captura |
| --- | --- |
| Sistema y CPU | Ubuntu 24.04 sobre WSL2, kernel 6.18.58.1, Ryzen 7 7435HS, 15 GiB visibles |
| Publicadores y consumidor | Python 3.12.3, Pika 1.4.4, google-cloud-pubsub 2.44.0 |
| RabbitMQ | 3.12.1, cola clásica durable, mensajes persistentes, confirmación por mensaje |
| Pub/Sub | Emulador 0.8.38, OpenJDK 21, gRPC local con un mensaje por solicitud |
| Carga | 1 024 bytes, cuatro publicadores, un consumidor, 32 mensajes de calentamiento por ensayo |
| Réplicas | Ocho bloques completos, orden aleatorio dentro de cada bloque, semilla 260613 |
| PDF | Quarto 1.11.9, Tectonic 0.17.0, IEEEtran v1.8b |
| SHA-256 de la captura | `47b8a7757fc6487a4e33de9a585f575d8e3c958179d49604c818a6723b3c4fca` |

## Reproducir el PDF a partir de la captura incluida

Instala Quarto y Tectonic y comprueba que sus ejecutables estén en PATH. Ejecuta desde la raíz. La primera compilación puede descargar recursos de LaTeX y el estilo CSL, fijado a un commit en `_quarto.yml`.

~~~bash
python -m venv /tmp/venv-brokers-u2
source /tmp/venv-brokers-u2/bin/activate
python -m pip install jupyter-core==5.9.1 nbclient==0.11.0 nbformat==5.11.1 ipykernel==7.4.0 matplotlib==3.11.1
export QUARTO_PYTHON="$(command -v python)"
export PYTHONDONTWRITEBYTECODE=1
quarto render articulo.qmd --to pdf
~~~

El documento vuelve a calcular tablas, gráfica, estimaciones pareadas y recuentos desde `results.csv`. El resumen y la interpretación corresponden a la captura archivada y deben revisarse si se sustituye por otra corrida.

## Repetir la medición

Instala RabbitMQ 3.12.1, OpenJDK 21, Google Cloud CLI y su componente [emulador de Pub/Sub](https://docs.cloud.google.com/pubsub/docs/emulator). Mantén ambos servicios y el cliente en el mismo entorno Linux. Los adaptadores esperan RabbitMQ local en `127.0.0.1:5672` con credenciales locales predeterminadas y el emulador en `127.0.0.1:8085`. La prueba crea y elimina recursos temporales propios.

~~~bash
# Con RabbitMQ iniciado, abre otra terminal para el emulador.
gcloud beta emulators pubsub start --project=benchmark-local --host-port=127.0.0.1:8085
~~~

~~~bash
source /tmp/venv-brokers-u2/bin/activate
python -m pip install pika==1.4.4 google-cloud-pubsub==2.44.0

# Prueba corta
python benchmark.py --rate 50 --seconds 1 --reps 1 --output /tmp/smoke-brokers-u2.csv

# Repetición completa, aproximadamente cuatro minutos
python benchmark.py --rate 1000 --seconds 2 --reps 8 --workers 4 --seed 260613 \
  --output /tmp/results-brokers-u2.csv
~~~

La semilla reproduce el orden de las condiciones. Los tiempos dependen del equipo, las versiones y la carga de fondo. Para volver a calcular el PDF con datos nuevos, sustituye `results.csv` en una copia de trabajo y contrasta la interpretación con los resultados recalculados.

## Diccionario de datos

| Campos | Significado |
| --- | --- |
| `broker, perfil, replica, orden, id` | Condición, bloque, orden del ensayo e identificador entero dentro del ensayo |
| `programado_ns, encolado_ns, inicio_ns, fin_rpc_ns` | Llegada prevista, ingreso en la cola del productor, inicio y fin de publicación, relativos al origen monotónico |
| `estado` | Confirmación positiva (`ok`) o clase del error de publicación registrado |
| `recepcion_ns, duplicados` | Primera recepción observada y repeticiones del identificador hasta el cierre de captura. El campo de recepción vacío indica ausencia en esa ventana |
| `fin_carga_ns, observacion_ns` | Fin del intervalo programado y cierre de captura |
| `tasa_media, segundos, workers, payload_bytes, semilla` | Parámetros de ejecución |

## Fuentes y alcance

El archivo BibTeX contiene seis publicaciones académicas y dos referencias técnicas primarias. Las publicaciones incluyen artículos de 2023 a 2026 en [Telecom](https://www.mdpi.com/journal/telecom/indexing), [Journal of Computer Sciences Institute](https://doaj.org/toc/2544-0764), [JUTI](https://doaj.org/toc/2406-8535) e [IEEE Micro](https://doi.org/10.1109/MM.2024.3420070), y un antecedente de [ACM DEBS](https://doi.org/10.1145/3093742.3093908). Las fichas de Google Cloud y RabbitMQ respaldan los detalles de implementación.

Para entregar en Moodle, pega la URL del repositorio público en el cuadro de texto en línea.
