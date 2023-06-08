from datetime import datetime

def get_fechahoy():
    """
    Devuelve un string con la fecha de hoy ordenada como dia, mes y año.
    """
    
    fecha = ''
    now = datetime.now()

    fecha = str(now.day)
    fecha += str(now.month)
    fecha += str(now.year)

    return fecha