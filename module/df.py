import pyodbc
from sqlalchemy import create_engine
import urllib
import pandas as pd


class OBJ():

    def __init__( self ):

    def write_sql( self, table, df ):

        df.to_sql( name = table, con = self.engine, index = False, if_exists = 'replace', index_label = None )