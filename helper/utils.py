import string
import random
import json

def random_string(size=6):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size))


def exception_handler(exceptionObj):
    """Function that takes exception Object(<Byte>,<str>) as a parameter and returns the error message<str>"""
    try:
        if isinstance(exceptionObj, Exception) and hasattr(exceptionObj, 'args'):
            if not (hasattr(exceptionObj, 'message' or hasattr(exceptionObj, 'msg'))):
                if len(exceptionObj.args) >= 1:
                    if type(exceptionObj.args[0]) == type(b''):
                        ob = json.loads(exceptionObj.args[0].decode('utf-8'))
                        if type(ob) == type({}) and ob['message']:
                            return ob['message']
                    else:

                        try:
                            if type(exceptionObj.args[0]) == type('') and exceptionObj.args[0][0] == 'b':
                                ob = json.loads(exceptionObj.args[0][2:-1])
                            else:
                                ob = json.loads(exceptionObj.args[0])
                            if type(ob) == type({}) and ob['message']:
                                try:
                                    return exception_handler(ob['message'])
                                except Exception as e:
                                    return ob['message']
                            elif type(ob) == type({}) and ob['msg']:
                                try:
                                    return exception_handler(ob['message'])
                                except Exception as e:
                                    return ob['msg']
                            return str(json.loads(exceptionObj.args[0]))
                        except Exception as e:
                            return str(exceptionObj.args[0])
            elif hasattr(exceptionObj, 'msg'):
                return exceptionObj.msg
            elif hasattr(exceptionObj, 'message'):
                return exceptionObj.message
        elif type(exceptionObj) == type(''):
            try:
                ob = json.loads(exceptionObj)
                if type(ob) == type({}):
                    if ob['message']:
                        return ob['message']
                    elif ob['msg']:
                        return ob['msg']
                    else:
                        return ob
            except Exception as e:
                return exceptionObj

    except Exception as e:
        return e

__all__ = [
    'random_string',
    'exception_handler'

]
