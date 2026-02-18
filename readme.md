# Análisis de Datos Bancarios - MongoDB Aggregation Framework

Este proyecto, desarrollado como parte del **Lab03**, consiste en un script de Node.js que realiza consultas complejas y procesamiento de datos sobre el dataset `sample_analytics` de MongoDB. El objetivo es transformar datos financieros en métricas de negocio utilizando el Aggregation Framework.

## Funcionalidades del Programa

El script ejecuta 10 consultas analíticas avanzadas directamente en la base de datos:

* **Rendimiento por Cliente:** Calcula el total de transacciones y el monto promedio gastado por usuario.
* **Segmentación de Cartera:** Clasifica a los clientes en categorías (Bajo, Medio, Alto) según su balance total.
* **Análisis Geográfico:** Identifica al cliente con el balance más alto en cada ciudad.
* **Filtrado Temporal:** Obtiene el Top 10 de transacciones más altas de los últimos meses registrados.
* **Cálculo de Variación:** Mide la variación porcentual entre la primera y la última transacción de cada cliente.
* **Reportes Mensuales:** Agrupa movimientos financieros por mes, año y tipo de transacción.
* **Gestión de Datos:** * Detecta clientes inactivos y los guarda en la colección `inactive_customers`.
    * Genera resúmenes por tipo de producto en `account_summaries`.
    * Filtra clientes de "Alto Valor" y los exporta a `high_value_customers`.
* **Frecuencia de Actividad:** Clasifica a los usuarios como *infrequent*, *regular* o *frequent* basándose en su promedio mensual de uso.



---

## Requisitos Técnicos

* **Entorno:** Node.js instalado.
* **Dependencias:** `mongodb` y `dotenv`.
* **Base de Datos:** Acceso a un clúster de MongoDB con las colecciones `customers`, `accounts` y `transactions`.

---

## Instalación y Uso

1. **Clonar el repositorio** y navegar a la carpeta del proyecto.
2. **Instalar dependencias:**
   ```bash
   npm install
3. **Configurar variables de entorno:**  
Crea un archivo `.env` en la raíz con tu URI de conexión:
    ```.env
    MONGODB_URI=tu_cadena_de_conexion_aquí

4. **Ejecutar el script:**
    ```Bash
    node index.js


## Desarrolladores
    Oli Viau - 23544
    Fabian Morales - 23267

Este proyecto utiliza recursos oficiales de documentación de MongoDB para operaciones CRUD y operadores de agregación ($lookup, $unwind, $sort).