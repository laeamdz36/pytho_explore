"""Decoradores para pretty print console for reports"""
import functools

def print_header(message):
    """Decorador para pasar como argumento un mensaje para el bloque 
        de impresion en consola
    """
    def envolve_block(func):
        """Decorador principal"""
        @functools.wraps(func)
        def wrapper(*args,**kwargs):
            msg = message.center(100,"-")
            nl = "\n"
            print(nl,msg,nl)
            f = func(*args,**kwargs)
            # print(nl,msg)
            return f
        return wrapper
    return envolve_block
