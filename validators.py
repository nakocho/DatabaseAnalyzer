import pandas as pd
import re
from email_validator import validate_email, EmailNotValidError
import logging

def validar_identificador(identificador):
    """
    Valida un identificador español (DNI, NIE o CIF).
    Devuelve una tupla (es_valido, mensaje_error).
    """
    identificador = str(identificador).strip().upper()

    if re.fullmatch(r'\d{8}[A-Z]', identificador):
        return validar_dni(identificador)
    elif re.fullmatch(r'[XYZ]\d{7}[A-Z]', identificador):
        return validar_nie(identificador)
    elif re.fullmatch(r'[ABCDEFGHJKLMNPQRSUVW]\d{7}[0-9A-J]', identificador):
        return validar_cif(identificador)
    else:
        return False, "Formato inválido para DNI/NIE/CIF"

def validar_dni(dni):
    letras_dni = "TRWAGMYFPDXBNJZSQVHLCKE"
    numero = int(dni[:-1])
    letra = dni[-1]
    letra_calculada = letras_dni[numero % 23]
    if letra == letra_calculada:
        return True, ""
    return False, f"Letra de control incorrecta (esperado: {letra_calculada})"

def validar_nie(nie):
    letras_dni = "TRWAGMYFPDXBNJZSQVHLCKE"
    prefijo = {'X': '0', 'Y': '1', 'Z': '2'}
    numero = int(prefijo[nie[0]] + nie[1:-1])
    letra = nie[-1]
    letra_calculada = letras_dni[numero % 23]
    if letra == letra_calculada:
        return True, ""
    return False, f"Letra de control incorrecta (esperado: {letra_calculada})"

def validar_cif(cif):
    letra_inicio = cif[0]
    numeros = cif[1:-1]
    control = cif[-1]

    suma_pares = sum(int(numeros[i]) for i in range(1, 7, 2))
    suma_impares = sum(sum(int(d) for d in str(int(numeros[i]) * 2)) for i in range(0, 7, 2))
    total = suma_pares + suma_impares
    control_num = (10 - (total % 10)) % 10
    control_letras = "JABCDEFGHI"

    if letra_inicio in "PQRSNW":
        esperado = control_letras[control_num]
        return (control == esperado,
                "" if control == esperado else f"Letra de control CIF incorrecta (esperado: {esperado})")
    elif letra_inicio in "ABEH":
        return (control == str(control_num),
                "" if control == str(control_num) else f"Dígito de control CIF incorrecto (esperado: {control_num})")
    else:
        valido = control == str(control_num) or control == control_letras[control_num]
        esperado = f"{control_num} o {control_letras[control_num]}"
        return (valido,
                "" if valido else f"Control CIF incorrecto (esperado: {esperado})")

def limpiar_y_elegir_telefono(telefono_str):
    """
    Clean and standardize phone numbers
    Prioritizes mobile numbers over landline numbers
    """
    if pd.isna(telefono_str):
        return ""
    
    # Split by common delimiters: / - ; , spaces
    candidatos = re.split(r"[\/\-;,\s]+", str(telefono_str).strip())
    
    # Clean and classify numbers
    moviles = []
    fijos = []

    for num in candidatos:
        solo_digitos = re.sub(r"\D", "", num)
        if len(solo_digitos) == 9:
            if solo_digitos.startswith(('6', '7')):
                moviles.append(solo_digitos)
            elif solo_digitos.startswith(('8', '9')):
                fijos.append(solo_digitos)
    
    # Prioritize mobile numbers
    if moviles:
        return moviles[0]
    elif fijos:
        return ""  # Return empty for landline as per original logic
    else:
        return ""

def validar_email(email_str):
    """
    Validate email addresses
    Returns tuple (is_valid, error_message, normalized_email)
    """
    if pd.isna(email_str) or str(email_str).strip() == "":
        return False, "Email vacío", "arabat@arabat.com", "null"
    
    email_str = str(email_str).strip()
    
    try:
        # Validate and normalize the email
        valid = validate_email(email_str)
        return True, "", valid.email, email_str  # Return normalized email and original input
    except EmailNotValidError as e:
        return False, str(e), "arabat@arabat.com", email_str  # Default email if invalid
    

# alternativa a validate_email

def validar_email_Regex(email_str):
    """
    Valida direcciones de email.
    - Primero comprueba formato con regex (rápido).
    - Solo valida formato.
    Devuelve:
        (es_valido, mensaje_error, email_normalizado, email_original)
    """
    EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")

    if pd.isna(email_str) or str(email_str).strip() == "":
        print("Email vacío")
        logging.info("Email vacío")
        return False, "Email vacío", "arabat@arabat.com", "null"
    
    email_str = str(email_str).strip()
    email_original = email_str # Guardamos el email original para devolverlo

    # Split by common delimiters: / - ; , spaces
    candidatos = re.split(r"[\/\-;,\s]+", str(email_str).strip())

    if len(candidatos) > 1:
        # Si hay más de un candidato, tomamos el primero
        email_str = candidatos[0]
        for email in candidatos:
            if EMAIL_REGEX.fullmatch(email):
                email_str = email
                break
            email_str = email


    if not EMAIL_REGEX.fullmatch(email_str):
        mail = email_str.upper()
        if "Ñ" in mail:
            return False, "Ñ no es un carácter válido en correos electrónicos", "arabat@arabat.com", email_original

        return False, "Formato de email inválido", "arabat@arabat.com", email_original
    else: 
        return True, "", email_str, email_original
