"""
Configuracion central del agente de control semanal de Reporte de Incompatibilidades.

Datos sensibles (emails, folder IDs) se cargan desde:
  - Variable de entorno OBRAS_CONFIG (GitHub Actions, desde Secret)
  - Archivo local config_obras.json (ejecucion local, gitignored)
"""
import os
import json

# Ruta base del proyecto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ============================================================================
# CARGAR DATOS SENSIBLES desde env var o archivo local
# ============================================================================
_CONFIG_FILE = os.path.join(BASE_DIR, "config_obras.json")


def _cargar_config_obras():
    """Carga la configuracion de obras desde env var o archivo local."""
    # 1. Intentar desde variable de entorno (GitHub Actions)
    env_data = os.environ.get("OBRAS_CONFIG")
    if env_data:
        return json.loads(env_data)

    # 2. Intentar desde archivo local
    if os.path.exists(_CONFIG_FILE):
        with open(_CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    raise FileNotFoundError(
        "No se encontro configuracion de obras.\n"
        "Asegurate de tener config_obras.json en la raiz del proyecto\n"
        "o la variable de entorno OBRAS_CONFIG configurada."
    )


_config = _cargar_config_obras()

# Ruta al proyecto con credenciales OAuth2 compartidas (configurable)
_CREDENTIALS_SIBLING = _config.get("credentials_sibling_dir", "gmail-credentials")
CREDENTIALS_DIR = os.path.join(os.path.dirname(BASE_DIR), _CREDENTIALS_SIBLING)

# Archivos de credenciales OAuth2
CREDENTIALS_FILE = os.path.join(CREDENTIALS_DIR, "credentials.json")
TOKEN_FILE = os.path.join(CREDENTIALS_DIR, "token.json")

# Scopes necesarios para Gmail API y Google Drive API
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Directorio temporal para descargar archivos adjuntos
TEMP_DIR = os.path.join(BASE_DIR, "temp_files")

# Directorio y archivos de reportes
REPORT_DIR = os.path.join(BASE_DIR, "reportes")
REPORT_JSON = os.path.join(REPORT_DIR, "reporte_incomp_data.json")
REPORT_TXT = os.path.join(REPORT_DIR, "reporte_incomp.txt")

# Registro de notificaciones enviadas (evita duplicados)
NOTIFICACIONES_JSON = os.path.join(REPORT_DIR, "notificaciones_enviadas.json")

# ============================================================================
# MODO PRUEBA - Enviar correos SOLO al usuario de prueba
# Cambiar a False para produccion
# ============================================================================
MODO_PRUEBA = False

TEST_EMAIL = _config["test_email"]
REPORTE_CC_EMAILS = _config["reporte_cc_emails"]
OBRAS = _config["obras"]

# Nombre de la empresa (para firmas de correo y reportes)
COMPANY_NAME = _config.get("company_name", "")

# Keywords para validar contenido del documento (datos de empresa al inicio)
KEYWORDS_EMPRESA = _config.get("keywords_empresa", [
    "incompatibilidad", "incompatibilidades",
    "reporte", "informe",
])

# ============================================================================
# ABREVIATURAS DE MESES EN ESPANOL (para carpetas de Google Drive)
# ============================================================================
MONTH_ABBREVS_ES = [
    "Ene", "Feb", "Mar", "Abr", "May", "Jun",
    "Jul", "Ago", "Sep", "Oct", "Nov", "Dic",
]

# ============================================================================
# Palabras clave para buscar correos de Reporte de Incompatibilidades en Gmail
# ============================================================================
SEARCH_KEYWORDS_INCOMP = [
    "reporte de incompatibilidad",
    "reporte de incompatibilidades",
    "informe de incompatibilidad",
    "informe de incompatibilidades",
    "incompatibilidades de obra",
    "incompatibilidad",
    "incompatibilidades",
    "incompat",
    "rep.incompat",
    "incomp. semanal",
    "incomp semanal",
]

# Extensiones validas de archivo adjunto (Excel o Word)
EXTENSIONES_VALIDAS = [".xlsx", ".xls", ".docx", ".doc"]


# Construir query de busqueda por remitentes
def _construir_emails_query():
    """Construye la parte FROM del query de Gmail con todos los emails de las obras."""
    todos_emails = []
    for obra in OBRAS.values():
        todos_emails.extend(obra["emails"])
    return " OR ".join(f"from:{email}" for email in todos_emails)


GMAIL_FROM_QUERY = _construir_emails_query()

# Query de busqueda por asunto
GMAIL_SUBJECT_QUERY = " OR ".join(
    f'subject:"{kw}"' for kw in SEARCH_KEYWORDS_INCOMP
)
