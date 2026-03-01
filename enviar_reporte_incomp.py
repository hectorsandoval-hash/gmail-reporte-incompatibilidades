"""
Generador y enviador de reporte HTML del control semanal de Incompatibilidades.
Genera un correo con 2 tablas:
  1. Resumen de cumplimiento (obras que enviaron + datos de formato)
  2. Obras que no enviaron (con estado de notificacion)

Se envian 2 versiones:
  - Reporte completo (con columna "Ver") para el usuario principal
  - Reporte compartido (sin columna "Ver") para destinatarios en copia
"""
import json
import base64
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta

from auth_gmail import autenticar_gmail, obtener_perfil
from config import REPORT_JSON, REPORTE_CC_EMAILS, MODO_PRUEBA, TEST_EMAIL, COMPANY_NAME

# Zona horaria Peru (UTC-5)
PERU_TZ = timezone(timedelta(hours=-5))


def _generar_estilos():
    """Genera los estilos CSS comunes para el reporte."""
    return """
  body { font-family: Arial, sans-serif; font-size: 12px; color: #333; }
  h2 { color: #1a73e8; border-bottom: 2px solid #1a73e8; padding-bottom: 5px; }
  h3 { color: #333; margin-top: 25px; margin-bottom: 8px; }
  table { border-collapse: collapse; width: 100%; margin: 10px 0; }
  th { background-color: #1a73e8; color: white; padding: 7px 8px; text-align: center; font-size: 11px; white-space: nowrap; }
  td { border: 1px solid #ddd; padding: 5px 8px; font-size: 11px; text-align: center; }
  tr:nth-child(even) { background-color: #f9f9f9; }
  .verde { color: #0d8043; font-weight: bold; }
  .rojo { color: #d93025; font-weight: bold; }
  .amarillo { color: #e37400; font-weight: bold; }
  .resumen-box { background: #e8f0fe; border-left: 4px solid #1a73e8; padding: 12px; margin: 15px 0; }
  .footer { color: #888; font-size: 10px; margin-top: 30px; border-top: 1px solid #ddd; padding-top: 10px; }
  a.ver-correo { color: #1a73e8; text-decoration: none; font-weight: bold; }
  a.ver-correo:hover { text-decoration: underline; }
  .badge-ok { background: #e6f4ea; color: #0d8043; padding: 2px 8px; border-radius: 3px; font-size: 10px; font-weight: bold; }
  .badge-error { background: #fce8e6; color: #d93025; padding: 2px 8px; border-radius: 3px; font-size: 10px; font-weight: bold; }
  .badge-warn { background: #fef7e0; color: #e37400; padding: 2px 8px; border-radius: 3px; font-size: 10px; font-weight: bold; }
  .badge-fmt-ok { background: #e6f4ea; color: #0d8043; padding: 2px 6px; border-radius: 3px; font-size: 10px; font-weight: bold; }
  .badge-fmt-err { background: #fce8e6; color: #d93025; padding: 2px 6px; border-radius: 3px; font-size: 10px; font-weight: bold; }
"""


def generar_cuerpo_email(resultado_cumplimiento, resultado_notificaciones, incluir_ver=True):
    """
    Genera el cuerpo HTML del reporte de Incompatibilidades.

    Args:
        resultado_cumplimiento: dict del agente de cumplimiento
        resultado_notificaciones: lista de resultados de notificaciones enviadas
        incluir_ver: si True, incluye la columna "Ver" con links a Gmail
    """
    cumplieron = resultado_cumplimiento["cumplieron"]
    tareo_incorrecto = resultado_cumplimiento["tareo_incorrecto"]
    no_enviaron = resultado_cumplimiento["no_enviaron"]
    rango_semana = resultado_cumplimiento["rango_semana"]
    total_obras = resultado_cumplimiento["total_obras"]

    total_ok = len(cumplieron)
    total_incorrecto = len(tareo_incorrecto)
    total_no_envio = len(no_enviaron)

    # Construir mapa de notificaciones por obra
    noti_map = {}
    for noti in (resultado_notificaciones or []):
        noti_map[noti["obra_key"]] = noti

    estilos = _generar_estilos()

    html = f"""
<html>
<head>
<style>
{estilos}
</style>
</head>
<body>

<h2>REPORTE DE INCOMPATIBILIDADES - CONTROL SEMANAL</h2>
<p>Semana del: <strong>{rango_semana}</strong></p>
<p>Reporte generado: <strong>{datetime.now(PERU_TZ).strftime('%d/%m/%Y %H:%M')}</strong></p>

<div class="resumen-box">
  <strong>RESUMEN EJECUTIVO</strong><br>
  Total obras monitoreadas: <strong>{total_obras}</strong><br>
  Cumplieron (reporte correcto): <span class="verde">{total_ok}</span><br>
  Enviaron con fecha incorrecta: <span class="amarillo">{total_incorrecto}</span><br>
  No enviaron: <span class="rojo">{total_no_envio}</span>
</div>
"""

    # ================================================================
    # TABLA 1: OBRAS QUE ENVIARON REPORTE
    # ================================================================
    col_ver_th = '<th>Ver</th>' if incluir_ver else ''
    total_cols = 10 if incluir_ver else 9

    html += f"""
<h3>1. OBRAS QUE ENVIARON REPORTE DE INCOMPATIBILIDADES</h3>
<table>
  <tr>
    <th>#</th>
    <th>Obra</th>
    <th>Correo Remitente</th>
    <th>Fecha Envio</th>
    <th>Fecha Reporte</th>
    <th>Reporte Correcto</th>
    <th>Tipo Archivo</th>
    <th>Formato</th>
    <th>G.Drive</th>
    {col_ver_th}
  </tr>
"""

    todos_enviaron = cumplieron + tareo_incorrecto
    todos_enviaron.sort(key=lambda x: x["obra_nombre"])

    for i, item in enumerate(todos_enviaron, 1):
        reporte = item.get("reporte", {})
        datos = item.get("datos", {})
        estado = item["estado"]

        fecha_doc_str = datos.get("fecha_documento") or "-"
        tipo_archivo = datos.get("tipo_archivo", "-")

        # Tipo de archivo badge
        if tipo_archivo == "excel":
            tipo_fmt = '<span style="color: #0d7c3f; font-weight: bold;">Excel</span>'
        elif tipo_archivo == "word":
            tipo_fmt = '<span style="color: #2b579a; font-weight: bold;">Word</span>'
        else:
            tipo_fmt = f'<span class="amarillo">{tipo_archivo}</span>'

        # Formato y Drive
        formato_ok = item.get("formato_ok", False)
        drive_ok = item.get("drive_ok", False)
        formato_fmt = '<span class="badge-fmt-ok">Ok</span>' if formato_ok else '<span class="badge-fmt-err">Corregir</span>'
        drive_fmt = '<span class="badge-fmt-ok">Ok</span>' if drive_ok else '<span class="badge-fmt-err">Falta</span>'

        if estado == "CUMPLIO":
            correcto_fmt = '<span class="verde">SI</span>'
        elif estado == "FECHA INCORRECTA":
            correcto_fmt = '<span class="rojo">NO</span>'
        else:
            correcto_fmt = '<span class="amarillo">-</span>'

        col_ver_td = ''
        if incluir_ver:
            gmail_link = reporte.get("gmail_link", "#")
            col_ver_td = f'<td><a class="ver-correo" href="{gmail_link}" target="_blank">Abrir</a></td>'

        html += f"""  <tr>
    <td>{i}</td>
    <td><strong>{item['obra_nombre']}</strong></td>
    <td>{reporte.get('de_email', '-')}</td>
    <td>{reporte.get('fecha_envio', '-')}</td>
    <td>{fecha_doc_str}</td>
    <td>{correcto_fmt}</td>
    <td>{tipo_fmt}</td>
    <td>{formato_fmt}</td>
    <td>{drive_fmt}</td>
    {col_ver_td}
  </tr>
"""

    if not todos_enviaron:
        html += f'<tr><td colspan="{total_cols}" class="rojo">Ninguna obra envio el reporte de incompatibilidades.</td></tr>'

    html += "</table>"

    # ================================================================
    # TABLA 2: OBRAS QUE NO ENVIARON
    # ================================================================
    html += """
<h3>2. OBRAS QUE NO ENVIARON REPORTE (LLAMADO DE ATENCION)</h3>
<table>
  <tr>
    <th>#</th>
    <th>Obra</th>
    <th>Correos Registrados</th>
    <th>G.Drive</th>
    <th>Comentarios</th>
    <th>Notificacion</th>
  </tr>
"""

    if no_enviaron:
        for i, item in enumerate(no_enviaron, 1):
            emails = ", ".join(item["emails_registrados"])
            noti = noti_map.get(item["obra_key"], {})
            noti_estado = noti.get("estado", "PENDIENTE")

            drive_ok = item.get("drive_ok", False)
            drive_fmt = '<span class="badge-fmt-ok">Ok</span>' if drive_ok else '<span class="badge-fmt-err">Falta</span>'

            if "ENVIADA" in noti_estado:
                noti_fmt = '<span class="verde">Correo enviado</span>'
            elif "YA NOTIFICADA" in noti_estado:
                noti_fmt = '<span class="verde">Ya notificada previamente</span>'
            elif "ERROR" in noti_estado:
                noti_fmt = f'<span class="rojo">{noti_estado}</span>'
            else:
                noti_fmt = '<span class="amarillo">Pendiente</span>'

            html += f"""  <tr>
    <td>{i}</td>
    <td><strong>{item['obra_nombre']}</strong></td>
    <td style="font-size: 10px;">{emails}</td>
    <td>{drive_fmt}</td>
    <td><span class="badge-error">NO ENVIO</span></td>
    <td>{noti_fmt}</td>
  </tr>
"""
    else:
        html += '<tr><td colspan="6" class="verde">Todas las obras enviaron su reporte de incompatibilidades.</td></tr>'

    html += f"""</table>

<div class="footer">
  Reporte generado automaticamente por Sistema Automatizado de Control y Gestion de Proyectos - {COMPANY_NAME}<br>
  Fecha: {datetime.now(PERU_TZ).strftime('%d/%m/%Y %H:%M')}<br>
  <em>Nota: "Reporte Correcto" indica si la fecha del reporte esta dentro de la semana revisada ({rango_semana}). El documento debe ser Excel o Word y contener datos de empresa y fecha al inicio.</em>
</div>

</body>
</html>
"""
    return html


def enviar_reporte(service, mi_email, resultado_cumplimiento, resultado_notificaciones):
    """
    Genera y envia el reporte por correo.
    - Reporte completo (con "Ver") al usuario principal
    - Reporte compartido (sin "Ver") a los destinatarios en CC
    """
    rango_semana = resultado_cumplimiento["rango_semana"]
    asunto = f"[REPORTE] Control Semanal de Incompatibilidades - Semana {rango_semana}"

    if MODO_PRUEBA:
        html_completo = generar_cuerpo_email(resultado_cumplimiento, resultado_notificaciones, incluir_ver=True)

        msg = MIMEText(html_completo, "html")
        msg["to"] = TEST_EMAIL
        msg["from"] = mi_email
        msg["subject"] = f"[PRUEBA] {asunto}"

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        sent = service.users().messages().send(
            userId="me", body={"raw": raw}
        ).execute()

        print(f"[OK] Reporte de PRUEBA enviado a {TEST_EMAIL} (ID: {sent['id']})")
        return sent

    # 1. Reporte completo para el usuario principal (con columna "Ver")
    html_completo = generar_cuerpo_email(resultado_cumplimiento, resultado_notificaciones, incluir_ver=True)

    msg = MIMEText(html_completo, "html")
    msg["to"] = mi_email
    msg["from"] = mi_email
    msg["subject"] = asunto

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    sent = service.users().messages().send(
        userId="me", body={"raw": raw}
    ).execute()

    print(f"[OK] Reporte enviado a {mi_email} (ID: {sent['id']})")

    # 2. Reporte compartido para CC (sin columna "Ver")
    if REPORTE_CC_EMAILS:
        html_compartido = generar_cuerpo_email(resultado_cumplimiento, resultado_notificaciones, incluir_ver=False)

        msg_cc = MIMEText(html_compartido, "html")
        msg_cc["to"] = ", ".join(REPORTE_CC_EMAILS)
        msg_cc["from"] = mi_email
        msg_cc["subject"] = asunto

        raw_cc = base64.urlsafe_b64encode(msg_cc.as_bytes()).decode("utf-8")
        sent_cc = service.users().messages().send(
            userId="me", body={"raw": raw_cc}
        ).execute()

        print(f"[OK] Reporte compartido enviado a {len(REPORTE_CC_EMAILS)} destinatarios (ID: {sent_cc['id']})")

    return sent


def main():
    """Ejecuta el envio de reporte desde los datos guardados en JSON."""
    service = autenticar_gmail()
    mi_email = obtener_perfil(service)
    print(f"Conectado como: {mi_email}")

    with open(REPORT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    resultado_cumplimiento = {
        "cumplieron": data.get("cumplieron", []),
        "tareo_incorrecto": data.get("tareo_incorrecto", []),
        "no_enviaron": data.get("no_enviaron", []),
        "fecha_inicio": data.get("fecha_inicio", ""),
        "fecha_fin": data.get("fecha_fin", ""),
        "rango_semana": data.get("rango_semana", ""),
        "total_obras": data.get("total_obras", 0),
    }
    resultado_notificaciones = data.get("notificaciones", [])

    print(f"Enviando reporte a {mi_email}...")
    enviar_reporte(service, mi_email, resultado_cumplimiento, resultado_notificaciones)
    print("Listo!")


if __name__ == "__main__":
    main()
