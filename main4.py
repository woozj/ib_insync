#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 19 17:50:58 2022

@author: woozhaojie
"""

from ib_insync import *
import os 
import configparser
import datetime
import logging
from pytz import timezone


logging.basicConfig(filename="traces.log", format='%(asctime)s %(message)s', filemode='w') 
logger=logging.getLogger() 
logger.setLevel(logging.INFO) 
tz = timezone('America/New_York')

class IbApp():
    def __init__(self):
        self.load_config()
        self.ib = IB()
        self.ib.connect(self.address, self.port, self.client_id)
        self.stocka = Stock(self.symbola, 'SMART', 'USD')
        self.stocka = self.ib.qualifyContracts(self.stocka)[0]
        self.stockb = Stock(self.symbolb, 'SMART', 'USD')
        self.stockb = self.ib.qualifyContracts(self.stockb)[0]
        self.stocka_dataa = None 
        self.stocka_datab = None 
        self.subscribeDataStockA()
        self.subscribeDataStockB()
        self.loop()

    def after_market_open(self):
        nw =datetime.datetime.now(tz)
        todayMarketOpen = datetime.datetime(year=nw.year, month=nw.month, day=nw.day, hour=9, minute=30, second=0)
        now = datetime.datetime(year=nw.year, month=nw.month, day=nw.day, hour=nw.hour, minute=nw.minute, second=nw.second)
        return ((now - todayMarketOpen).total_seconds() > 0)
    
    def before_market_close(self):
        nw =datetime.datetime.now(tz)
        todayMarketClose = datetime.datetime(year=nw.year, month=nw.month, day=nw.day, hour=15, minute=59, second=59)
        now = datetime.datetime(year=nw.year, month=nw.month, day=nw.day, hour=nw.hour, minute=nw.minute, second=nw.second)
        return ((now - todayMarketClose).total_seconds() < 0)

    def close_positions(self, stocka, stockb):
        if len(self.ib.positions()) > 0:
            for position in self.ib.positions():
                if position.contract.symbol == stocka.symbol:
                    if position.position > 0:
                        order = MarketOrder('SELL', position.position)
                        self.ib.placeOrder(stocka, order)
                    else:
                        order = MarketOrder('BUY', abs(position.position))
                        self.ib.placeOrder(stocka, order)

                if position.contract.symbol == stockb.symbol:
                    if position.position > 0:
                        order = MarketOrder('SELL', position.position)
                        self.ib.placeOrder(stockb, order)
                    else:
                        order = MarketOrder('BUY', abs(position.position))
                        self.ib.placeOrder(stockb, order)
        self.ib.sleep(5)
    
    def reqPnL(self, account: str, modelCode: str = '') -> PnL:
        key = (account, modelCode)
        assert key not in self.wrapper.pnlKey2ReqId
        reqId = self.client.getReqId()
        self.wrapper.pnlKey2ReqId[key] = reqId
        pnl = PnL(account, modelCode)
        self.wrapper.reqId2PnL[reqId] = pnl
        self.client.reqPnL(reqId, account, modelCode)
        return pnl
    
    def get_pnl(self, stock, price):
        if len(self.ib.positions()) > 0:
            for position in self.ib.positions():
                if position.contract.symbol == stock.symbol:
                    return position.position * (price - position.avgCost)
        return 0

    def go_long(self, stock, price):
        size = int(self.cash / price)
        order = MarketOrder('BUY', size)
        self.ib.placeOrder(stock, order)

    def go_short(self, stock, price):
        size = int(self.cash / price)
        order = MarketOrder('Sell', size)  
        self.ib.placeOrder(stock, order)

    def loop(self):
        last_update_time = datetime.datetime.now()
        last_update_algo = datetime.datetime(2000, 1, 1)

        while True:
            self.ib.sleep(0.01)
            # update spread calculation 
            if self.stocka_dataa is not None and self.stocka_datab is not None and self.after_market_open() and self.before_market_close():
                if self.stocka_dataa["date"].iloc[-1] == self.stocka_datab["date"].iloc[-1] and last_update_algo !=  self.stocka_datab["date"].iloc[-1]:
                    last_update_algo = self.stocka_datab["date"].iloc[-1]
                    stocka_close = self.stocka_dataa["close"].iloc[-2]
                    stockb_close = self.stocka_datab["close"].iloc[-2]
                    stocka_close_ma = self.stocka_dataa["close"].iloc[-61:-1].mean()
                    stockb_close_ma = self.stocka_datab["close"].iloc[-61:-1].mean()

                    stocka_close_prev = self.stocka_dataa["close"].iloc[-3]
                    stockb_close_prev = self.stocka_datab["close"].iloc[-3]
                    stocka_close_ma_prev = self.stocka_dataa["close"].iloc[-62:-2].mean()
                    stockb_close_ma_prev = self.stocka_datab["close"].iloc[-62:-2].mean()

                    spread = ((8058/(stocka_close * 32540)) - (6536/(stockb_close * 12703)))
                    spread_ma = ((8058/(stocka_close_ma * 32540)) - (6536/(stockb_close_ma * 12703))) + 0.0025
                    #spread_ma2 = ((8058/(stocka_close_ma * 32540)) - (6536/(stockb_close_ma * 12703))) - 0.0015

                    spread_prev = ((8058/(stocka_close_prev * 32540)) - (6536/(stockb_close_prev * 12703)))
                    spread_ma_prev = ((8058/(stocka_close_ma_prev * 32540)) - (6536/(stockb_close_ma_prev * 12703))) + 0.0025
                    #spread_ma_prev2 = ((8058/(stocka_close_ma_prev * 32540)) - (6536/(stockb_close_ma_prev * 12703))) - 0.0015
                    
                    pnla = self.get_pnl(self.stocka, self.stocka_dataa["close"].iloc[-1])
                    pnlb = self.get_pnl(self.stockb, self.stocka_datab["close"].iloc[-1])
                    pnl = round(pnla + pnlb, 2)
                          
                    logger.info(self.symbola + " close: " + str(stocka_close))
                    logger.info(self.symbolb + " close: " + str(stockb_close))
                    logger.info("spread: " + str(spread))
                    logger.info("spread MA: " + str(spread_ma))
                    #logger.info("spread MA2: " + str(spread_ma2))

                    logger.info(self.symbola + " close prev: " + str(stocka_close_prev))
                    logger.info(self.symbolb + " close prev: " + str(stockb_close_prev))
                    logger.info("spread prev: " + str(spread_prev))
                    logger.info("spread MA prev: " + str(spread_ma_prev))
                    #logger.info("spread MA prev2: " + str(spread_ma_prev2))

                    if (spread_prev < spread_ma_prev) and (spread > spread_ma):
                        #cross up, long stock A, short Stock B 
                        logger.info("Cross Up")
                        #self.close_positions(self.stocka, self.stockb)
                        self.go_long(self.stocka, self.stocka_dataa["close"].iloc[-1])
                        self.go_short(self.stockb, self.stocka_datab["close"].iloc[-1])
   
                    if (pnl > 100):
                        #cross down, Short stock A, long stock B
                        logger.info("Cross Down")
                        self.close_positions(self.stocka, self.stockb)
                        #self.go_long(self.stockb, self.stocka_datab["close"].iloc[-1])
                        #self.go_short(self.stocka, self.stocka_dataa["close"].iloc[-1])

                # Print State To Console 
                if (datetime.datetime.now() - last_update_time).total_seconds() >= 0.9:
                    last_update_time = datetime.datetime.now()
                    pnla = self.get_pnl(self.stocka, self.stocka_dataa["close"].iloc[-1])
                    pnlb = self.get_pnl(self.stockb, self.stocka_datab["close"].iloc[-1])
                    pnl = round(pnla + pnlb, 2)
                    print(datetime.datetime.now(), "PNL: $", pnl)


    def load_config(self):
        config = configparser.ConfigParser()
        p = os.path.dirname(os.path.realpath(__file__))
        config.read(p + '/cfg.ini')
        self.address = config.get("General", "Address")
        self.port = config.get("General", "Port")
        self.client_id = config.get("General", "ClientId")
        self.symbola = config.get("General", "SymbolA")
        self.symbolb = config.get("General", "SymbolB")
        self.cash = float(config.get("General", "Order_Cash"))
        
    def subscribeDataStockA(self):
        historical_data_15secs = self.ib.reqHistoricalData(self.stocka, '', barSizeSetting='1 min', durationStr='2 D', whatToShow='TRADES', useRTH=True, keepUpToDate=True)
        self.stocka_dataa = util.df(historical_data_15secs)
        historical_data_15secs.updateEvent += self.onNewDataStockA

    def onNewDataStockA(self, bars, hasNewBar):
        if hasNewBar:
            self.stocka_dataa = util.df(bars)

    def subscribeDataStockB(self):
        historical_data_15secs = self.ib.reqHistoricalData(self.stockb, '', barSizeSetting='1 min', durationStr='2 D', whatToShow='TRADES', useRTH=True, keepUpToDate=True)
        self.stocka_datab = util.df(historical_data_15secs)
        historical_data_15secs.updateEvent += self.onNewDataStockB

    def onNewDataStockB(self, bars, hasNewBar):
        if hasNewBar:
            self.stocka_datab = util.df(bars)
        

app = IbApp()