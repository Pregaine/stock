import os
import re
import glob

path = 'C:/workspace/data/tmp\\全台卷商交易資料_'

for file in glob.glob( path + "*/*.csv" ):

    file = file.replace( path, '' )
    src = file.split( '\\' )[ 1 ]
    tar = file.split( '\\' )[ 0 ]
    date = src[ -12:-4 ]

    stock = re.match( '\d{4,6}', src )
    stock = stock.group( )

    name = src.split( '_' )[ 0 ]
    name = name.replace( stock, '' )
    new = '{0}{1}\{2}_{3}_{4}.csv'.format( path, tar, stock, name, date )

    try:
        os.rename( path + file, new )
    except Exception as e:
        print( '修改失敗{}', path + file )