import pandas as pd
import re
from email_validator import validate_email, EmailNotValidError

def validar_identificador(identificador):
    """
    Validate Spanish identification documents (DNI, NIE, CIF)
    Returns tuple (is_valid, error_message)
    """
    identificador = str(identificador).strip().upper()
    letras_dni = "TRWAGMYFPDXBNJZSQVHLCKE"

    # DNI validation
    if re.fullmatch(r'\d{8}[A-Z]', identificador):
        numero = int(identificador[:-1])
        letra = identificador[-1]
        letra_calculada = letras_dni[numero % 23]
        if letra == letra_calculada:
            return True, ""
        else:
            return False, f"Letra de control incorrecta (esperado: {letra_calculada})"

    # NIE validation
    elif re.fullmatch(r'[XYZ]\d{7}[A-Z]', identificador):
        prefijo = {'X': '0', 'Y': '1', 'Z': '2'}
        numero = int(prefijo[identificador[0]] + identificador[1:-1])
        letra = identificador[-1]
        letra_calculada = letras_dni[numero % 23]
        if letra == letra_calculada:
            return True, ""
        else:
            return False, f"Letra de control incorrecta (esperado: {letra_calculada})"

    # CIF validation
    elif re.fullmatch(r'[ABCDEFGHJKLMNPQRSUVW]\d{7}[0-9A-J]', identificador):
        letra_inicio = identificador[0]
        numeros = identificador[1:-1]
        control = identificador[-1]

        suma_pares = sum(int(numeros[i]) for i in range(1, 7, 2))
        suma_impares = 0
        for i in range(0, 7, 2):
            doble = str(int(numeros[i]) * 2)
            suma_impares += sum(int(d) for d in doble)
        total = suma_pares + suma_impares
        control_num = (10 - (total % 10)) % 10
        control_letras = "JABCDEFGHI"

        if letra_inicio in "PQRSNW":
            esperado = control_letras[control_num]
            if control == esperado:
                return True, ""
            else:
                return False, f"Letra de control CIF incorrecta (esperado: {esperado})"
        elif letra_inicio in "ABEH":
            if control == str(control_num):
                return True, ""
            else:
                return False, f"Dígito de control CIF incorrecto (esperado: {control_num})"
        else:
            if control == str(control_num) or control == control_letras[control_num]:
                return True, ""
            else:
                return False, f"Control CIF incorrecto (esperado: {control_num} o {control_letras[control_num]})"

    return False, "Formato inválido para DNI/NIE/CIF"

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
        return False, "Email vacío", ""
    
    email_str = str(email_str).strip()
    
    try:
        # Validate and normalize the email
        valid = validate_email(email_str)
        return True, "", valid.email
    except EmailNotValidError as e:
        return False, str(e), ""
