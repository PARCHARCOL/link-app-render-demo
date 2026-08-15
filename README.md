# Link

Aplicacion web online para Link con backend Node en Render.

## Que incluye

- Logo oficial en el encabezado.
- API real en `/api/state`.
- Consulta automatica de fuentes oficiales en `/api/official-news`.
- Filtro estricto para publicaciones relacionadas con casinos, bingos, tragamonedas y juegos localizados.
- Formularios para publicar informacion, perfiles, productos y conversaciones.
- Datos guardados en `data/link-data.json` dentro del servicio.

## Render

Este repo usa `render.yaml` como Blueprint.

- Plan: Free
- Runtime: Node
- Build Command: `npm install`
- Start Command: `npm start`
- Health Check: `/health`

Nota: esta persistencia por archivo sirve para prueba online. Para produccion permanente conviene conectar PostgreSQL.
