# -*- coding: utf-8 -*-
import stock_inquire.stock_inquire as siq
import stock_db.

cmd = '''
    CREATE TABLE dbo.BrokerageAvgPrice
	(
	stock varchar(10) NOT NULL,
	date date NOT NULL,
	avg_buy_price decimal(6, 2) NULL,
	avg_sell_price decimal(6, 2) NULL,
	rate decimal(3, 2) NULL
	)  ON [PRIMARY] 
	'''
