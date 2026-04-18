# 🏢 Carga de Empresas en la Base de Datos

Guía rápida para cargar una empresa en el entorno de desarrollo utilizando pruebas automatizadas.

---

## 📁 1. Preparar los archivos requeridos

Asegúrate de tener los siguientes archivos y colocarlos en la ubicación correspondiente:

- 📄 `certificado.cer`
- 🔐 `llave.key`
- 📝 `pass.txt`

> ⚠️ **Importante:** Los nombres de los archivos deben mantenerse exactamente como se indica y alamcenarse en en la ruta: tests/load_data/company_to_load/fiel

---
## 📁 2. Carga de archivos XML

Asegúrate de obtener los archivos XML y colocarlos en la carpeta 'xmls' en la ruta: tests/load_data/xmls

---

## 🧪 3. Ejecutar pruebas para cargar datos

Para cargar los datos de la empresa, puedes usar los siguientes tests:

- ▶️ **Carga básica:**
  Ejecutar el test `test_load_xmls` **sin necesidad de generar metadata**.

- 📦 **Carga con metadata (si es necesario):**
  En caso de necesitar también la generación de metadata, ejecutar `test_load_xmls_with_metadata`.

---

## 📝 Notas

- 🔧 Al ejecutar `test_load_xmls`, **tener en cuenta** que debe estar definida la variable de entorno:
  ```bash
  XML_CREATE_RECORDS=1
    ```

## 🚀 4. Ejecutar pruebas apuntando a diferentes bases de datos

### 🧪 Base de datos (Persistente)

#### ▶️ Carga básica (sin metadata)

```bash
PERSIST_TESTS=true poetry run pytest tests/load_data/test_load_xml.py::test_load_xmls
```

#### ▶️ Carga básica (sin metadata)

```bash
PERSIST_TESTS=true poetry run pytest tests/load_data/test_load_xml.py::test_load_xmls_with_metadata
```
## 📝 Notas

Si no deseas persistir ejecuta el comando sin la variable de entorno:    PERSIST_TESTS=true
