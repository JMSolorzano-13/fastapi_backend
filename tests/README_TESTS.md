# Variables de entorno
Las siguientes variables de entorno las podemos definir en elarchivo .env o al ejecutar uno o varios test con el comando pytest.

```sql
PYTHONPATH=.
```

Creación de base de datos

```sql
create database local;
create database local_test;
```

# Alembic

Generar tablas en **base de datos local**

```sql
poetry run alembic -c alembic.ini upgrade head
```

Generar tablas en **base de datos test**


# Ejecución de Tests

## Persistente

```sql
poetry run pytest --commit nombre_test
```

## Test

```sql
poetry run pytest nombre_test
```

## Test para creación de empresa y metadata

[Ir al archivo](load_data/instrucciones.md)
