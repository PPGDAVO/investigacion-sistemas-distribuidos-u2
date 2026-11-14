# Bitácora de trazabilidad y uso de IA

**Fecha:** 13 de noviembre de 2026  
**Herramientas:** OpenCode CLI 1.19.5 con Qwen2.5-Coder 1.5B Instruct local mediante llama.cpp, y Codex para implementar, ejecutar, analizar y redactar el borrador. OpenCode se ejecutó con herramientas y edición deshabilitadas. El proveedor gratuito remoto rechazó una solicitud por límite de uso y se completaron las dos consultas en local.

## Prompts ejecutados en OpenCode

### 1. Diseño de medición y código

> Diseña un benchmark Python de lazo abierto exclusivamente local para comparar RabbitMQ AMQP y el emulador Google Cloud Pub/Sub gRPC. Incluye un fragmento mínimo con perf_counter_ns, confirma publicación y separa retraso de cola y no recepción por plazo. Responde en máximo 250 palabras, sin herramientas.

**Respuesta observada.** La salida propuso pasos de edición en Bash y repitió el uso de `perf_counter_ns`. Omitió el fragmento Python y sugirió confirmar la publicación «using perf_counter_ns».

**Corrección aplicada.** El reloj registra tiempos y la confirmación proviene del broker. La implementación usa confirmaciones AMQP con enrutamiento obligatorio y valida el identificador devuelto por gRPC. Las llegadas programadas alimentan una FIFO independiente. El consumidor registra el primer identificador observado, cuenta repeticiones y envía el acuse. La captura conserva tiempos de encolado, inicio, respuesta y recepción para distinguir los estados al corte.

### 2. Quarto y bibliografía

> Da un ejemplo mínimo de Quarto para PDF con IEEEtran en modo conference y figura Python con #| label: fig-resultados y #| fig-cap:. Incluye un registro BibTeX estructural con DOI de ejemplo sin inventar referencias reales. Responde en máximo 200 palabras, sin herramientas.

**Respuesta observada.** El agente repitió YAML con `output: pdf_document` y `format: "conference"`, e incluyó CSS y JavaScript que no correspondían al PDF. Omitió el bloque Python y el registro BibTeX.

**Corrección aplicada.** Se declaró `format.pdf.documentclass: IEEEtran` con `classoption: [conference, letterpaper]`. Se añadió un bloque ejecutable que genera la figura a partir del CSV y se compiló el documento con Tectonic. Se consultaron las páginas de los editores para registrar los DOI en BibTeX y se cotejaron los detalles de implementación con la documentación oficial.

## Verificaciones realizadas

- La ejecución corta completó ambos perfiles en ambos brokers. La captura completa contiene 32 ensayos, 2 000 identificadores únicos por ensayo y 64 000 filas de datos.
- Se comprobó el orden de las marcas del publicador y que toda recepción registrada ocurrió entre el inicio de la publicación y el cierre de captura.
- Quarto ejecutó los bloques de análisis sobre el CSV archivado. Se revisaron el conteo de páginas, la figura, las tablas y la bibliografía en el PDF.

## Declaración de integridad académica

La IA participó en el diseño preliminar, el código, la verificación y la redacción del borrador. Los registros provienen de llamadas ejecutadas contra RabbitMQ y el emulador oficial local de Pub/Sub. La bitácora documenta las respuestas de OpenCode y las correcciones aplicadas durante la preparación.
