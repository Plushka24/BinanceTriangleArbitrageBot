import requests, json
import time
import threading
from PyQt5 import QtWidgets
from MainWindow import Ui_MainWindow
import sys
import asyncio
import aiohttp
from coins import data as for_reqs
from qty import qtys 
import traceback
import logging
from binance.spot import Spot
from datetime import datetime
import gspread



class ApplicationWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super(ApplicationWindow, self).__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.for_reqs = for_reqs
        keys_list = []
        self.api_keys_check = False
        keys = open('keys.txt', encoding = 'utf-8', errors='ignore')
        for k in keys:
            k = k.replace('\n', '')
            keys_list.append(k)
        try:
            self.api_key = keys_list[0]
            self.private_key = keys_list[1]
            self.ui.lineEdit_4.setText(self.api_key)
            self.ui.lineEdit_5.setText(self.private_key)
            self.time = datetime.now()
            self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [INFO]| Получены ключи.")
        except:
            self.api_keys_check = True
        self.set()
        
    def set(self):
        self.ui.pushButton.clicked.connect(lambda: self.start())


    def start(self):
        self.findSpread_1 = float(self.ui.lineEdit.text())
        self.trade_volume = float(self.ui.lineEdit_3.text())
        self.table_url = str(self.ui.lineEdit_7.text())
        if self.api_keys_check:
            self.api_key = self.ui.lineEdit_4.text()
            self.private_key = self.ui.lineEdit_5.text()
            self.time = datetime.now()
            self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [INFO]| Получены ключи.")

        if self.api_key == '':
            self.time = datetime.now()
            self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [ERROR]| Не задан API key.")
        elif self.private_key == '':
            self.time = datetime.now()
            self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [ERROR]| Не задан PRIVATE key.")
        else:
            threading.Thread(target=self.start_scan).start()
            

    def start_scan(self):
        self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [INFO]| Ждём 90 сек.")
        time.sleep(90)
        if self.table_url:
            self.gc = gspread.service_account(filename='auth.json')
            try:
                self.sh = self.gc.open_by_url(self.table_url)
                self.worksheet = self.sh.get_worksheet(0)
            except:
                self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [ERROR]| Неверный url таблицы")
                return
        session = requests.Session()
        self.time = datetime.now()
        self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [INFO]| Поиск актуальных связок...")
        while True:
            self.trade_volume = float(self.ui.lineEdit_3.text())
            count = 0
            spreads = {}
            dataDict = {}
            database = {}
            with_btc = []
            with_eth = []
            with_bnb = []
            
            for x in self.for_reqs:
                if x.find("BTC") != -1:
                    with_btc.append(x.replace('BTC', ''))
                elif x.find("ETH") != -1:
                    with_eth.append(x.replace('ETH', ''))
                elif x.find("BNB") != -1:
                    with_bnb.append(x.replace('BNB', ''))



            async def get_data(session, link):
                try:
                    async with session.get("https://api.binance.com/api/v3/depth?symbol="+link+"&limit=10") as resp:
                        resp_text =  await resp.json()
                        database[link] = resp_text
                except:
                    pass
            async def data():
                links = self.for_reqs
                async with aiohttp.ClientSession() as session:
                    tasks = []
                    for link in range(len(links)):
                        task = asyncio.create_task(get_data(session, links[link]))
                        tasks.append(task)
                    await asyncio.gather(*tasks)
            
            asyncio.run(data())
            try:
                for d in database:
                    buy_orders = []
                    sell_orders = []
                    for i in range(len(database[d]['bids'])):
                        buy_orders.append(database[d]['bids'][i][0])
                    for i in range(len(database[d]['asks'])):
                        sell_orders.append(database[d]['asks'][i][0])
                    dataDict[d] = buy_orders, sell_orders
            except:
                self.time = datetime.now()
                self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [INFO]| Блокировка запросов. Ожидание 20 минут.")
                time.sleep(1200)
                self.time = datetime.now()
                self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [INFO]| Работа возобновлена.")
                continue

            for x in dataDict:
                if x == "BTCUSDT":
                    for i in range(len(dataDict[x][0])):
                        first_price = dataDict[x][0][i]
                        first_price = float(first_price)
                        for y in with_btc:
                            for j in range(len(dataDict[x][0])):
                                for k in range(len(dataDict[x][0])):
                                    try:
                                        if dataDict[y+"BTC"]:
                                            second_price = dataDict[y+"BTC"][0][j]
                                            second_price = float(second_price)
                                    except:
                                        continue
                                    try:
                                        if dataDict[y+"USDT"]:
                                            third_price = dataDict[y+"USDT"][1][k]
                                            third_price = float(third_price)
                                    except:
                                        continue
                                    try:
                                        volume = ((float(self.trade_volume)/float(first_price))/float(second_price))*float(third_price)
                                        if volume > float(self.trade_volume):
                                            self.first_volume = float(self.trade_volume)/float(first_price)
                                            self.second_volume = self.first_volume/float(second_price)
                                            self.pair_1 = "BTCUSDT"
                                            self.pair_2 = y+"BTC"
                                            self.pair_3 = y+"USDT"
                                            spread = ((volume / self.trade_volume)*100)-100-0.3
                                            if spread > self.findSpread_1:
                                                count+=1
                                                spread = round(spread, 2)
                                                spreads[spread] = self.pair_1, self.pair_2, self.pair_3, first_price, second_price, third_price, self.first_volume, self.second_volume 
                                    

                                    except Exception as e:
                                        self.ui.textEdit.append('[ERROR]| '+str(e))
                                        continue
                elif x == "ETHUSDT":
                    for i in range(len(dataDict[x][0])):
                        first_price = dataDict[x][0][i]
                        first_price = float(first_price)
                        for y in with_eth:
                            for j in range(len(dataDict[x][0])):
                                for k in range(len(dataDict[x][0])):
                                    try:
                                        if dataDict[y+"ETH"]:
                                            second_price = dataDict[y+"ETH"][0][j]
                                            second_price = float(second_price)
                                    except:
                                        continue
                                    try:
                                        if dataDict[y+"USDT"]:
                                            third_price = dataDict[y+"USDT"][1][k]
                                            third_price = float(third_price)
                                    except:
                                        continue
                                    try:
                                        volume = ((float(self.trade_volume)/float(first_price))/float(second_price))*float(third_price)
                                        if volume > float(self.trade_volume):
                                            self.first_volume = float(self.trade_volume)/float(first_price)
                                            self.second_volume = self.first_volume/float(second_price)
                                            self.pair_1 = "ETHUSDT"
                                            self.pair_2 = y+"ETH"
                                            self.pair_3 = y+"USDT"
                                            spread = ((volume / self.trade_volume)*100)-100-0.3
                                            if spread > self.findSpread_1:
                                                count+=1
                                                spread = round(spread, 2)
                                                spreads[spread] = self.pair_1, self.pair_2, self.pair_3, first_price, second_price, third_price, self.first_volume, self.second_volume 
                                    

                                    except Exception as e:
                                        self.ui.textEdit.append('[ERROR]| '+str(e))
                                        continue
                elif x == "BNBUSDT":
                    for i in range(len(dataDict[x][0])):
                        first_price = dataDict[x][0][i]
                        first_price = float(first_price)
                        for y in with_bnb:
                            for j in range(len(dataDict[x][0])):
                                for k in range(len(dataDict[x][0])):
                                    try:
                                        if dataDict[y+"BNB"]:
                                            second_price = dataDict[y+"BNB"][0][j]
                                            second_price = float(second_price)
                                    except:
                                        continue
                                    try:
                                        if dataDict[y+"USDT"]:
                                            third_price = dataDict[y+"USDT"][1][k]
                                            third_price = float(third_price)
                                    except:
                                        continue
                                    try:
                                        volume = ((float(self.trade_volume)/float(first_price))/float(second_price))*float(third_price)
                                        if volume > float(self.trade_volume):
                                            self.first_volume = float(self.trade_volume)/float(first_price)
                                            self.second_volume = self.first_volume/float(second_price)
                                            self.pair_1 = "BNBUSDT"
                                            self.pair_2 = y+"BNB"
                                            self.pair_3 = y+"USDT"
                                            spread = ((volume / self.trade_volume)*100)-100-0.3
                                            if spread > self.findSpread_1:
                                                count+=1
                                                spread = round(spread, 2)
                                                spreads[spread] = self.pair_1, self.pair_2, self.pair_3, first_price, second_price, third_price, self.first_volume, self.second_volume 
                                    
                                    except Exception as e:
                                        self.ui.textEdit.append('[ERROR]| '+str(e))
                                        continue
            if count > 0:
                add = []
                for i in spreads:
                    add.append(i)
                max_spread = max(add)
                signal = spreads[max_spread]
                self.pair_1 = spreads[max_spread][0]
                self.pair_2 = spreads[max_spread][1]
                self.pair_3 = spreads[max_spread][2] 
                self.price_1 = float(spreads[max_spread][3])
                self.price_2 = float(spreads[max_spread][4])
                self.price_3 = float(spreads[max_spread][5])
                x = 0
                y = 0
                for i in qtys:
                    if i == self.pair_1:
                        x = qtys[i]
                    if i == self.pair_2:
                        y = qtys[i]
                self.first_volume = str(spreads[max_spread][6])
                self.second_volume = str(spreads[max_spread][7])
                self.first_volume = float(self.first_volume[:self.first_volume.find('.')+x])
                self.second_volume = float(self.second_volume[:self.second_volume.find('.')+y])
                #print("Spread: "+ str(max_spread)+"|"+str(signal)+"|"+str(self.first_volume)+" "+str(self.second_volume))
                self.time = datetime.now()
                self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [INFO]| Найдено связок: "+str(count)+"\n"
                                        +"Выбрана связка с максимальным спредом: "+str(max_spread)+"%")
                self.trade()
                time.sleep(60)
            else:
                self.time = datetime.now()
                self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [INFO]| Связок не найдено...Следующее обновление через 60 сек.")
                time.sleep(60)
    def trade(self):
        client = Spot(key=self.api_key, secret=self.private_key)
    ###################################Первый блок
        params = {
            "symbol": self.pair_1,
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": self.first_volume,
            "price": self.price_1,
            "recvWindow": 60000
            }
        try:
            response = client.new_order(**params)
            order_id = response['orderId']
        except Exception as e:
            e = eval(str(e))
            if e[1] == -2010:
                self.ui.textEdit.append('[ERROR]| Объём покупки превышает доступный баланс.')
                return
            self.ui.textEdit.append('[ERROR]| '+str(e))
            return
        check = True
        self.time = datetime.now()
        self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [TRADE]| Создан ордер на покупку: "+self.pair_1)
        while check:
            params = {
            "recvWindow": 60000
            }
            try:
                response = client.get_order(self.pair_1, orderId=order_id, **params)
                if response['status'] == 'FILLED':
                    self.time = datetime.now()
                    self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [TRADE]| Ордер "+self.pair_1+"(покупка) исполнен."+"\n"
                                            "Цена: "+str(self.price_1)+" | Объём: "+str(self.first_volume))
                    check = False
                else:
                    time.sleep(10)
                    continue
            except Exception as e:
                self.ui.textEdit.append('[ERROR]| '+str(e))
                return
    ###################################Первый блок
    ###################################Второй блок
        params = {
            "symbol": self.pair_2,
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": self.second_volume,
            "price": self.price_2,
            "recvWindow": 60000
            }
        try:
            response = client.new_order(**params)
            order_id = response['orderId']
        except Exception as e:
            self.ui.textEdit.append('[ERROR]| '+str(e))
            return
            
        check = True
        self.time = datetime.now()
        self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [TRADE]| Создан ордер на покупку: "+self.pair_2)
        while check:
            params = {
            "recvWindow": 60000
            }
            try:
                response = client.get_order(self.pair_2, orderId=order_id, **params)
                if response['status'] == 'FILLED':
                    self.time = datetime.now()
                    self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [TRADE]| Ордер "+self.pair_2+"(покупка) исполнен."+"\n"
                                            "Цена: "+str(self.price_2)+" | Объём: "+str(self.second_volume))
                    check = False
                else:
                    time.sleep(10)
                    continue
            except Exception as e:
                self.ui.textEdit.append('[ERROR]| '+str(e))
                return
    ###################################Второй блок
    ###################################Третий блок
        params = {
            "symbol": self.pair_3,
            "side": "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": self.second_volume,
            "price": self.price_3,
            "recvWindow": 60000
            }
        try:
            response = client.new_order(**params)
            order_id = response['orderId']
        except Exception as e:
            self.ui.textEdit.append('[ERROR]| '+str(e))
            return
            
        check = True
        self.time = datetime.now()
        self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [TRADE]| Создан ордер на продажу: "+self.pair_3)
        while check:
            params = {
            "recvWindow": 60000
            }
            try:
                response = client.get_order(self.pair_3, orderId=order_id, **params)
                if response['status'] == 'FILLED':
                    self.time = datetime.now()
                    self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [TRADE]| Ордер "+self.pair_3+"(покупка) исполнен."+"\n"
                                            "Цена: "+str(self.price_3)+" | Объём: "+str(self.second_volume))
                    check = False
                else:
                    time.sleep(10)
                    continue
            except Exception as e:
                self.ui.textEdit.append('[ERROR]| '+str(e))
                return
    ###################################Третий блок
        self.time = datetime.now()
        self.ui.textEdit.append(str(self.time.strftime('%Y-%m-%d %H.%M.%S'))+" [TRADE]| Закончен круг: "+self.pair_1+" -> "+self.pair_2+" -> "+self.pair_3)
        if self.table_url:
            body=[self.pair_1+" -> "+self.pair_2+" -> "+self.pair_3, "закончен"]
            self.worksheet.append_row(body, table_range="A1:B1")

            
if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    application = ApplicationWindow()
    application.show()
    sys.exit(app.exec_())
