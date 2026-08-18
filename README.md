# Link

Aplicacion web online para Link con backend Node en Render.

## Que incluye

- Logo oficial en el encabezado.
- API real en `/api/state`.
- Consulta automatica de fuentes oficiales en `/api/official-news`.
- Filtro estricto para publicaciones relacionadas con casinos, bingos, tragamonedas y juegos localizados.
- Registro con correo y clave.
- Tipo de cuenta: persona natural o empresa.
- Bolsa de Empleo con hojas de vida, vacantes y CV imprimible en `/cv/:id`.
- Formularios protegidos para publicar informacion, productos y conversaciones.
- Modulo Admin para cambiar logo visible, revisar usuarios y aprobar/ocultar/eliminar publicaciones.
- Persistencia en PostgreSQL cuando existe `DATABASE_URL`; fallback local en `data/link-data.json` para pruebas.

## Render

Este repo usa `render.yaml` como Blueprint.

- Plan: Free
- Runtime: Node
- Build Command: `npm install`
- Start Command: `npm start`
- Health Check: `/health`

## PostgreSQL

Crear una base en Render llamada `link-db` y copiar su `Internal Database URL` en el servicio web como:

- Key: `DATABASE_URL`
- Value: Internal Database URL de `link-db`

Al redeploy, la app crea automaticamente las tablas `link_users`, `link_sessions`, `link_resumes`, `link_vacancies`, `link_news`, `link_products`, `link_threads` y `link_messages`.
