from falkonryclient.helper import schema as schemas
from falkonryclient.service import falkonry


client = falkonry.FalkonryService
__all__ = [
  'client',
  'schemas'
]
__title__     = 'falkonryclient'
__version__   = '2.2.1'
__author__    = 'Falkonry Inc'
__copyright__ = 'Copyright 2016-2018 Falkonry Inc'
__license__   = """