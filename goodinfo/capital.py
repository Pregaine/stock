import pandas as pd

class capital:

    def __init__( self, path = 'C:\workspace\stock\goodinfo\StockList_股本.csv' ):

        df = pd.read_csv( path, lineterminator = '\n', encoding = 'utf8', sep = ',', index_col = False, na_filter = False, thousands =','  )

        df[ '代號' ] = df[ '代號' ].str.strip( '="' )
        df[ 'stock' ]= df[ '代號' ].astype( str )
        del df[ '代號' ]

        df[ '股本(億)' ] = df[ '股本(億)\r' ]
        del df[ '股本(億)\r' ]

        df.sort_values( by=['股本(億)'], ascending=False, inplace = True )
        df.reset_index( inplace = True )

        print( '{0:<8}股本 {1:>5} (億)'.format( df.loc[ 0,  '名稱' ], df.loc[ 0, '股本(億)' ] ) )
        print( '{0:<8}股本 {1:>5} (億)'.format( df.loc[ 1, '名稱' ], df.loc[ 1, '股本(億)' ] ) )
        print( '{0:<8}股本 {1:>5} (億)'.format( df.loc[ 2, '名稱' ], df.loc[ 2, '股本(億)' ] ) )

        self.df = df

def main():

    capital_obj = capital( )

if __name__ == '__main__':

    main()
