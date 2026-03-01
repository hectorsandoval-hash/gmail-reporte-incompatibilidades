"""
AGENTE 1: Busqueda de Correos de Reporte de Incompatibilidades
- Busca correos con asunto relacionado a incompatibilidades en Gmail
- Filtra por remitentes conocidos (obras registradas en config)
- Filtra por rango semanal (lunes a domingo de la semana anterior)
- Mapea cada correo a su obra correspondiente
- Detecta links de Google Drive/Sheets en el cuerpo del correo
"""
import base64
import re
from email.utils import parseaddr
from datetime import datetime, date, timedelta

from config import OBRAS, GMAIL_FROM_QUERY, GMAIL_SUBJECT_QUERY, EXTENSIONES_VALIDAS


def buscar_reportes(service, fecha_inicio, fecha_fin):
    """
    Busca correos de Reporte de Incompatibilidades en Gmail para un rango semanal.

    Args:
        service: Gmail API service
        fecha_inicio: date object (lunes de la semana a revisar)
        fecha_fin: date object (domingo de la semana a revisar)

    Returns:
        Lista de diccionarios con info de cada correo encontrado
    """
    # Construir query de fecha (after/before en formato YYYY/MM/DD)
    fecha_after = fecha_inicio.strftime("%Y/%m/%d")
    fecha_before = (fecha_fin + timedelta(days=1)).strftime("%Y/%m/%d")

    # Query: de los remitentes conocidos + asunto de incompatibilidades + rango de fecha
    query = f"({GMAIL_FROM_QUERY}) ({GMAIL_SUBJECT_QUERY}) after:{fecha_after} before:{fecha_before}"

    print(f"\n[AGENTE 1] Buscando reportes de incompatibilidades")
    print(f"[AGENTE 1] Rango: {fecha_inicio.strftime('%d/%m/%Y')} al {fecha_fin.strftime('%d/%m/%Y')}")
    print(f"[AGENTE 1] Query: {query[:150]}...")

    resultados = []
    page_token = None

    while True:
        response = (
            service.users()
            .messages()
            .list(
                userId="me",
                q=query,
                maxResults=50,
                pageToken=page_token,
            )
            .execute()
        )

        messages = response.get("messages", [])
        if not messages:
            break

        for msg_ref in messages:
            msg_data = _procesar_mensaje(service, msg_ref["id"])
            if msg_data:
                resultados.append(msg_data)

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    # Si hay multiples correos de la misma obra, tomar el mas reciente
    resultados_por_obra = {}
    for r in resultados:
        key = r["obra_key"]
        if key not in resultados_por_obra:
            resultados_por_obra[key] = r
        else:
            # Comparar fechas y quedarse con el mas reciente
            existente = resultados_por_obra[key]
            if r["fecha_raw"] > existente["fecha_raw"]:
                resultados_por_obra[key] = r

    resultados_unicos = list(resultados_por_obra.values())

    print(f"[AGENTE 1] Se encontraron {len(resultados)} correos totales, {len(resultados_unicos)} obras unicas.")
    return resultados_unicos


def _procesar_mensaje(service, message_id):
    """Procesa un mensaje individual y extrae informacion relevante."""
    msg = (
        service.users()
        .messages()
        .get(userId="me", id=message_id, format="full")
        .execute()
    )

    headers = msg.get("payload", {}).get("headers", [])
    header_dict = {h["name"].lower(): h["value"] for h in headers}

    asunto = header_dict.get("subject", "(Sin asunto)")
    de = header_dict.get("from", "")
    fecha_raw = header_dict.get("date", "")

    # Extraer email del remitente
    _, de_email = parseaddr(de)
    de_email_lower = de_email.lower()

    # Mapear remitente a obra
    obra_key, obra_nombre = _mapear_email_a_obra(de_email_lower)
    if not obra_key:
        return None  # Remitente no reconocido

    # Parsear fecha de envio
    fecha_envio = _parsear_fecha(fecha_raw)

    # Buscar adjuntos Excel/Word
    payload = msg.get("payload", {})
    adjuntos = _buscar_adjuntos_documento(payload)

    # Buscar links de Google Drive/Sheets en el cuerpo del correo
    links_drive = _buscar_links_drive(payload)

    # Link directo a Gmail
    gmail_link = f"https://mail.google.com/mail/u/0/#all/{msg['id']}"

    return {
        "id": msg["id"],
        "thread_id": msg.get("threadId", ""),
        "obra_key": obra_key,
        "obra_nombre": obra_nombre,
        "de": de,
        "de_email": de_email_lower,
        "asunto": asunto,
        "fecha_envio": fecha_envio,
        "fecha_raw": fecha_raw,
        "tiene_adjunto_documento": len(adjuntos) > 0 or len(links_drive) > 0,
        "adjuntos": adjuntos,
        "links_drive": links_drive,
        "gmail_link": gmail_link,
    }


def _mapear_email_a_obra(email_lower):
    """Mapea un email de remitente a su obra correspondiente."""
    for key, obra in OBRAS.items():
        for email_obra in obra["emails"]:
            if email_lower == email_obra.lower():
                return key, obra["nombre"]
    return None, None


def _buscar_adjuntos_documento(payload, adjuntos=None):
    """Busca recursivamente adjuntos Excel o Word en el payload del mensaje."""
    if adjuntos is None:
        adjuntos = []

    filename = payload.get("filename", "")
    attachment_id = payload.get("body", {}).get("attachmentId")

    if filename and attachment_id:
        if any(filename.lower().endswith(ext) for ext in EXTENSIONES_VALIDAS):
            adjuntos.append({
                "filename": filename,
                "attachmentId": attachment_id,
                "mimeType": payload.get("mimeType", ""),
            })

    for part in payload.get("parts", []):
        _buscar_adjuntos_documento(part, adjuntos)

    return adjuntos


def _buscar_links_drive(payload):
    """
    Busca links de Google Drive/Sheets/Docs en el cuerpo del correo.
    Extrae el file ID de URLs como:
      - https://docs.google.com/spreadsheets/d/FILE_ID/edit...
      - https://docs.google.com/document/d/FILE_ID/edit...
      - https://drive.google.com/file/d/FILE_ID/view...
      - https://drive.google.com/open?id=FILE_ID
    """
    body_text = _extraer_body_texto(payload)
    if not body_text:
        return []

    links = []
    seen_ids = set()

    # Patron para Google Sheets/Docs/Drive
    patrones = [
        r'https?://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]+)',
        r'https?://docs\.google\.com/document/d/([a-zA-Z0-9_-]+)',
        r'https?://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)',
        r'https?://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)',
    ]

    for patron in patrones:
        for match in re.finditer(patron, body_text):
            file_id = match.group(1)
            if file_id not in seen_ids:
                seen_ids.add(file_id)

                # Determinar tipo por URL
                url = match.group(0)
                if 'spreadsheets' in url:
                    tipo = 'google_sheets'
                elif 'document' in url:
                    tipo = 'google_docs'
                else:
                    tipo = 'google_drive'

                links.append({
                    "file_id": file_id,
                    "url": url,
                    "tipo": tipo,
                })

    return links


def _extraer_body_texto(payload):
    """Extrae el texto plano del body del correo (recursivo)."""
    parts_text = []

    mime = payload.get("mimeType", "")
    body_data = payload.get("body", {}).get("data", "")

    if body_data and ("text/plain" in mime or "text/html" in mime):
        try:
            decoded = base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")
            parts_text.append(decoded)
        except Exception:
            pass

    for part in payload.get("parts", []):
        parts_text.append(_extraer_body_texto(part))

    return "\n".join(parts_text)


def _parsear_fecha(fecha_raw):
    """Parsea la fecha del header del correo a formato legible."""
    formatos = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
    ]
    fecha_limpia = re.sub(r"\s*\([^)]*\)\s*$", "", fecha_raw).strip()

    for fmt in formatos:
        try:
            dt = datetime.strptime(fecha_limpia, fmt)
            return dt.strftime("%d/%m/%Y %H:%M")
        except ValueError:
            continue
    return fecha_raw
