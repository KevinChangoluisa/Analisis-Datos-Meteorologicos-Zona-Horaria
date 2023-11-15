# Análisis de Datos Meteorológicos para Determinar la Zona Horaria

Este repositorio contiene una solución de análisis de datos meteorológicos desarrollada en Python. La aplicación se utiliza para determinar la zona horaria de las estaciones meteorológicas en tiempo real.

## Descripción

El objetivo principal de esta aplicación es recopilar y analizar datos procedentes de estaciones meteorológicas y determinar con precisión la zona horaria correspondiente a cada estación. La aplicación se ejecuta en tiempo real y utiliza las siguientes etapas de procesamiento de datos:

1. **Obtención de Estaciones Meteorológicas**: La aplicación obtiene una lista actualizada de estaciones meteorológicas que están transmitiendo datos continuamente.

2. **Recopilación de Datos de Temperatura Instantánea**: Para cada estación identificada, se recopilan los datos de temperatura instantánea registrados en las últimas 24 horas.

3. **Cálculo de Temperaturas Máximas y Mínimas**: Se calculan las temperaturas máximas y mínimas durante ese período.

4. **Análisis de Zona Horaria**: La aplicación realiza un análisis de las horas en las que se registran las temperaturas máximas y mínimas, comparándolas con franjas horarias típicas para determinar con precisión la zona horaria correspondiente, ya sea la hora local o UTC.

5. **Registro y Almacenamiento de Información**: Se registra y almacena la información de la estación, las temperaturas máximas y mínimas y la zona horaria detectada.

## Configuración de Base de Datos

La aplicación utiliza bases de datos PostgreSQL y MongoDB para almacenar y recuperar datos relevantes. Asegúrate de configurar adecuadamente el archivo `config.ini` en el directorio `utils` con las credenciales de las bases de datos, siguiendo el formato proporcionado.

```ini
[DBPOSTGRESINAMHI]
USERNAME = tu_usuario
PASSWORD = tu_contraseña
HOST = tu_host
PORT = tu_puerto
DB = tu_base_de_datos

[DBPOSTGRESKEVIN]
USERNAME = tu_usuario
PASSWORD = tu_contraseña
HOST = tu_host
PORT = tu_puerto
DB = tu_base_de_datos

[DBMONGOINAMHI]
USERNAME = tu_usuario
PASSWORD = tu_contraseña
HOST = tu_host
PORT = tu_puerto
DB = tu_base_de_datos
