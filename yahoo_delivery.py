# -*- coding:utf-8 -*-
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
import json
from datetime import datetime, timedelta
import logging
import sys
import linecache
import os
import cx_Oracle
import uvicorn
import hashlib 
from enum import Enum
import re
from fastapi.middleware.cors import CORSMiddleware

from public.yahoo_api_utils import YahooApiUtils as yh_utils
from public.yahoo_api_dao import YahooApiDao
from public.custom_exceptions import *
from public.project_variables import *

from models.product_factory import ProductFactory

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

User = 'taaze_web'
Pwd = 'XSX12345'

class Object:
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)

def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    logging.error('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))

def settingLog():
    # 設定
    datestr = datetime.today().strftime('%Y%m%d')
    if not os.path.exists("log/" + datestr):
        os.makedirs("log/" + datestr)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%y-%m-%d %H:%M:%S',
                        handlers = [logging.FileHandler('log/' + datestr + '/yahoo_sp.log', 'a', 'utf-8'),])

    
    # 定義 handler 輸出 sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # 設定輸出格式
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # handler 設定輸出格式
    console.setFormatter(formatter)
    # 加入 hander 到 root logger
    logging.getLogger('').addHandler(console)

def xstr(s) -> str:
    if s is None:
        return ''
    mystr = ""
    try:
        if type(s) is str:
            # print('s = ', type(s))
            mystr = str(s)
        elif type(s) is cx_Oracle.BLOB:
            mystr = s.read()
            # mystr = str(s)
        elif type(s) is cx_Oracle.LOB:
            # print('lob = ' + s.read())
            mystr = s.read()
            # print('lob = ', s)
            # mystr = s.read()
        elif type(s) is cx_Oracle.CLOB:
            mystr = s.read()
        elif type(s) is float:
            mystr = str(s)
        else:
            mystr = s
            # print('s = ', type(s))
        #     print('s = '+ str(s))
    except:
        print('except row = ', type(s))
        PrintException()
        pass
    # rtnstr = (mystr[:16380] + '..') if len(mystr) > 16382 else mystr
    rtnstr = mystr
    return rtnstr

@app.get('/store_delivery')
async def store_delivery(spNo: str, request: Request):
        
    if not spNo:
        raise HTTPException(status_code=401, detail="Missing parameter")

    with YahooApiDao() as master_dao, YahooApiDao() as detail_dao, YahooApiDao() as operations_dao:
        
        master_dao.get_yahoo_sp_main(spNo)

        spConfirmFlg = ""
        try:
            row = master_dao.fetchone()
            while row:
                spConfirmFlg = row['SP_CONFIRM_FLG']
                if spConfirmFlg == 'B':
                    detail_dao.get_yahoo_sp_detail(spNo)

                    TransactionId = ""
                    CurrentOrderId = ""
                    OrderId = []
                    try:
                        row_sp = detail_dao.fetchone()
                        while row_sp:
                            TransactionId = row_sp['TRANSACTIONID']
                            if CurrentOrderId != row_sp['ORDERID']:
                                CurrentOrderId = row_sp['ORDERID']
                                OrderId.append(row_sp['ORDERID'])

                            row_sp = detail_dao.fetchone()
                        
                        receipt = yh_utils.confirm_store_delivery(TransactionId, OrderId)
                        logging.info('store_delivery:' + spNo + ' ' + str(receipt))
                        #receipt = {'Response': {'@Status': 'ok', 'DeliveryReceipt': {'@Id': 'S834018097679', 'StoreType': 'Family', 'DeliveryReceiptPrice': '302', 'OrderShippingConfirmDate': '2022/07/12', 'LastDeliveryDate': '2022/07/13', 'OrderDelayShippingDate': '2022/07/16', 'DeliveryToConvenienceStoreDate': '2022/07/14', 'ConvenienceStoreReturnDate': '2022/07/21', 'OrderList': {'Order': [{'@Id': 'YM2207125003861'}, {'@Id': 'YM2207125003862'}]}}, '_Message': ''}}
                        
                        if receipt['Response']['@Status'] == "ok":
                            DeliveryReceipt = receipt['Response']['DeliveryReceipt']
                            detail = yh_utils.get_store_delivery_receipt_detail(DeliveryReceipt['@Id'])
                            #detail = {'Response': {'@Status': 'ok', 'DeliveryReceipt': {'@Id': 'S834018097679', 'TransactionId': '220698230', 'StoreType': 'Family', 'SerialNumber': '61285225', 'DeliveryReceiptPrice': '302', 'DistributionChannelStatus': '0', 'OrderShippingConfirmDate': '2022/07/12', 'LastDeliveryDate': '2022/07/13', 'OrderDelayShippingDate': '2022/07/16', 'TakeDeliveryConvenienceStoreId': 'F017623', 'Barcode': '834S834018097679', 'FirstBarcode': '834S834018097679', 'RoutingBarcode': '1US05', 'PickupEShopBarcode': '834027901', 'PickupCodBarcode': '2266442510030269', 'PickupLogisticCode': '18340002722664425D', 'LogisticCode': '18340002722664425', 'LogisticCheckSum': 'D', 'StoreEquimentId': '7', 'StoreRegion': '南', 'StoreRoute': 'US', 'StoreRouteTrim': '05', 'MobilePhone': '253', 'ConvenienceStoreId': 'F017623', 'OrderInformation': '2206982301597', 'LogisticOrderNumber': '02722664425', 'DistributionCenter': 'DRE', 'CustomerInformation': '(02)7722-9966 / tw.mall.yahoo.com', 'Remark': '請收款結帳', 'OfficialName': '雅虎奇摩', 'DeliveryToConvenienceStoreDate': '2022/07/14', 'ConvenienceStoreReturnDate': '2022/07/21', 'ConvenienceStoreOrderId': '220698230-61285225', 'SenderMobile': '', 'OrderList': {'@Count': '2', 'Order': [{'@Id': 'YM2207125003861', 'OrderStatus': 'NEW', 'OrderProductList': {'@Count': '1', 'Product': {'@Id': '1020623', 'SaleType': 'Normal', 'Amount': '1', 'Subtotal': '50', '_CustomizeProductId': '', '_ProductName': '物流服務費（全家取貨付款）', '_Spec': '-'}}, '_DeliverType': '全家(物流交寄)', '_OrderStatusDesc': '未結案'}, {'@Id': 'YM2207125003862', 'OrderStatus': 'NEW', 'OrderProductList': {'@Count': '1', 'Product': {'@Id': 'p069929984284_1', 'SaleType': 'Normal', 'Amount': '1', 'Subtotal': '252', '_CustomizeProductId': '11100649090', '_ProductName': '國中英語單字八週完補計畫', '_Spec': '-'}}, '_DeliverType': '全家(物流交寄)', '_OrderStatusDesc': '未結案'}]}, '_DistributionChannelStatusDesc': '產生出貨單', '_ReceiverName': '紀瑞榮', '_TakeDeliveryConvenienceStoreName': '全家澎湖百利店', '_QrCode': 'B1||                  ||         ||                  ||               ||18340002722664425D||2||017623||1US05|| 7||0||834027901||2266442510030269||             ||          ', '_ReturnPeriod': '當日退', '_ReturnType': '其他-佶傳', '_ConvenienceStoreName': '全家澎湖百利店', '_StoreName': 'TAAZE 讀冊生活網路書店', '_CustomerCareInfo': '0266177599#205', '_Url': 'tw.mall.yahoo.com', '_SenderName': ''}, '_Message': ''}}
                            logging.info('SP_NO:' + spNo + ', id:' + DeliveryReceipt['@Id'] + ': ' + str(detail))

                            if detail['Response']['@Status'] == "ok":
                                rcp = detail['Response']['DeliveryReceipt']
                                
                                HilifeStoreId = ""
                                
                                ConvenienceStoreOrderId = rcp['ConvenienceStoreOrderId']
                                if ConvenienceStoreOrderId[0:1] == 'F':
                                    ConvenienceStoreOrderId = ConvenienceStoreOrderId[1:]
                                else:
                                    HilifeStoreId = ConvenienceStoreOrderId
                                
                                # 20200206 fix for yahoo pass old stno
                                tokens = rcp['_QrCode'].split("||")
                                if len(tokens) > 7:
                                    stno = tokens[7]
                                else:
                                    stno = rcp['TakeDeliveryConvenienceStoreId']

                                #if rcp['StoreType'] == 'HiLife':
                                #    StoreRoute = rcp['StoreRoute']

                                Area = rcp.get('StoreRegion', '')
                                if len("".join([x if ord(x) < 128 else 'xx' for x in Area])) > 1:
                                	Area = "1"

                                operations_dao.update_yahoo_sp_main(sp_no=spNo,
                                                                TransactionId=rcp['TransactionId'],
                                                                Id=rcp['@Id'],
                                                                StoreType = rcp['StoreType'],
                                                                SerialNumber = rcp['SerialNumber'],
                                                                DeliveryReceiptPrice = rcp['DeliveryReceiptPrice'],
                                                                DistributionChannelStatus = rcp['DistributionChannelStatus'],
                                                                _DistributionChannelStatusDesc = rcp['_DistributionChannelStatusDesc'],
                                                                OrderShippingConfirmDate = rcp['OrderShippingConfirmDate'],
                                                                LastDeliveryDate = rcp['LastDeliveryDate'],
                                                                OrderDelayShippingDate = rcp['OrderDelayShippingDate'],
                                                                _ReceiverName = rcp['_ReceiverName'],
                                                                _TakeDeliveryConvenienceStoreName = rcp['_TakeDeliveryConvenienceStoreName'],
                                                                TakeDeliveryConvenienceStoreId = rcp['TakeDeliveryConvenienceStoreId'],
                                                                Barcode = rcp['Barcode'],
                                                                DeliveryToConvenienceStoreDate = rcp['DeliveryToConvenienceStoreDate'],
                                                                ConvenienceStoreReturnDate = rcp['ConvenienceStoreReturnDate'],
                                                                ConvenienceStoreOrderId = ConvenienceStoreOrderId,
                                                                _StoreName = rcp['_StoreName'],
                                                                _CustomerCareInfo = rcp['_CustomerCareInfo'],
                                                                _Url = rcp['_Url'],
                                                                FirstBarcode = rcp['FirstBarcode'],
                                                                RoutingBarcode = rcp['RoutingBarcode'],
                                                                PickupEShopBarcode = rcp['PickupEShopBarcode'],
                                                                PickupCodBarcode = rcp['PickupCodBarcode'],
                                                                PickupLogisticCode = rcp.get('PickupLogisticCode', ''),
                                                                LogisticCode = rcp.get('LogisticCode', ''),
                                                                LogisticCheckSum = rcp.get('LogisticCheckSum', ''),
                                                                _QrCode = rcp['_QrCode'],
                                                                StoreEquimentId = rcp['StoreEquimentId'],
                                                                StoreRegion = Area,
                                                                StoreRoute = rcp['StoreRoute'],
                                                                StoreRouteTrim = rcp['StoreRouteTrim'],
                                                                _ReturnPeriod = rcp['_ReturnPeriod'],
                                                                _ReturnType = rcp['_ReturnType'],
                                                                MobilePhone = rcp['MobilePhone'],
                                                                ConvenienceStoreId = rcp['ConvenienceStoreId'],
                                                                _ConvenienceStoreName = rcp['_ConvenienceStoreName'],
                                                                OrderInformation = rcp.get('OrderInformation', ''),
                                                                LogisticOrderNumber = rcp['LogisticOrderNumber'],
                                                                DistributionCenter = rcp.get('DistributionCenter', ''),
                                                                )

                                operations_dao.update_yahoo_sp_detail(sp_no=spNo,
                                                                TransactionId=rcp['TransactionId'],
                                                                Id=rcp['@Id'],
                                                                )

                                if rcp['StoreType'] != 'HiLife':
                                    operations_dao.update_sp_main(sp_no = spNo,
                                                                Id = rcp['@Id'],
                                                                _ReceiverName = rcp['_ReceiverName'],
                                                                _TakeDeliveryConvenienceStoreName = rcp['_TakeDeliveryConvenienceStoreName'],
                                                                TakeDeliveryConvenienceStoreId = stno if HilifeStoreId != '' else 'F' + stno,  # rcp['TakeDeliveryConvenienceStoreId'], # 20200206 fix for yahoo pass old stno
                                                                Barcode = rcp['Barcode'],
                                                                DeliveryToConvenienceStoreDate = rcp['DeliveryToConvenienceStoreDate'],
                                                                ConvenienceStoreReturnDate = rcp['ConvenienceStoreReturnDate'],
                                                                ConvenienceStoreOrderId = ConvenienceStoreOrderId,
                                                                _StoreName = rcp['_StoreName'],
                                                                _CustomerCareInfo = rcp['_CustomerCareInfo'],
                                                                _Url = rcp['_Url'],
                                                                StoreRegion = Area,
                                                                StoreRoute = rcp['StoreRoute'],
                                                                StoreRouteTrim = rcp['StoreRouteTrim'],
                                                                _QrCode = rcp['_QrCode']
                                                                )
                                else:
                                    operations_dao.update_sp_main_hilife(sp_no = spNo,
                                                                Id = rcp['@Id'],
                                                                _ReceiverName = rcp['_ReceiverName'],
                                                                _TakeDeliveryConvenienceStoreName = rcp['_TakeDeliveryConvenienceStoreName'],
                                                                TakeDeliveryConvenienceStoreId = stno if HilifeStoreId != '' else 'F' + stno,  # rcp['TakeDeliveryConvenienceStoreId'], # 20200206 fix for yahoo pass old stno
                                                                Barcode = rcp['Barcode'],
                                                                DeliveryToConvenienceStoreDate = rcp['DeliveryToConvenienceStoreDate'],
                                                                ConvenienceStoreReturnDate = rcp['ConvenienceStoreReturnDate'],
                                                                ConvenienceStoreOrderId = ConvenienceStoreOrderId,
                                                                _StoreName = rcp['_StoreName'],
                                                                _CustomerCareInfo = rcp['_CustomerCareInfo'],
                                                                _Url = rcp['_Url'],
                                                                StoreRegion = Area,
                                                                StoreRoute = rcp['StoreRoute'],
                                                                StoreRouteTrim = rcp['StoreRouteTrim'],
                                                                _QrCode = rcp['_QrCode']
                                                                )
                                operations_dao.commit_changes()
                            else:
                                operations_dao.update_yahoo_sp_main_fail(sp_no=spNo, status='F')
                                operations_dao.update_yahoo_sp_detail_fail(sp_no=spNo, status='F')
                                operations_dao.update_sp_main_fail(sp_no = spNo, status='F')
                                operations_dao.commit_changes()
                                obj = {
                                    "RETURNCODE":"1004", 
                                    "RETURNMSG":"駁回"
                                    }
                                return obj
                                
                        else:
                            operations_dao.update_yahoo_sp_main_fail(sp_no=spNo, status='C')
                            operations_dao.update_yahoo_sp_detail_fail(sp_no=spNo, status='D')
                            operations_dao.update_sp_main_fail(sp_no = spNo, status='B')
                            operations_dao.commit_changes()
                            
                            msg = "關店"
                            
                            if receipt['Response']['ErrorMessage'] == "部分訂單已取消，請再確認":
                                msg = "顧客訂單取消"
                                
                            obj = {
                                "RETURNCODE":"1003", 
                                "RETURNMSG": msg
                                }
                            return obj
                            
                    except Exception as e:
                        error, = e.args
                        logging.error(error)
                        #logging.error(error.message)
                        #logging.error(error.context)
                        msg_log = "store_delivery FAIL : %s" % (error.message) 
                        logging.info(msg_log)
                        obj = {
                            "RETURNCODE":"1001", 
                            "RETURNMSG":error.message
                            }
                        return obj
                else:
                    obj = {
                            "RETURNCODE":"1002", 
                            "RETURNMSG":"該筆資料不合法"
                            }
                    return obj

                row = detail_dao.fetchone()
        except Exception as e:
            error, = e.args
            logging.error(error)
            #logging.error(error.message)
            #logging.error(error.context)
            msg_log = "store_delivery FAIL : %s" % (error.message) 
            logging.info(msg_log)
            obj = {
                    "RETURNCODE":"1001", 
                    "RETURNMSG":error.message
                    }
            return obj

    obj = {
            "RETURNCODE":"1000", 
            "RETURNMSG":"完成"
            }
    return obj

@app.get('/home_delivery')
async def home_delivery(spNo: str, request: Request):
        
    if not spNo:
        raise HTTPException(status_code=401, detail="Missing parameter")

    with YahooApiDao() as master_dao, YahooApiDao() as detail_dao, YahooApiDao() as operations_dao:
        
        master_dao.get_yahoo_sp_main(spNo)

        spConfirmFlg = ""
        try:
            row = master_dao.fetchone()
            while row:
                spConfirmFlg = row['SP_CONFIRM_FLG']
                shippingNo = row['SHIPPINGNUMBER']

                if spConfirmFlg == 'B':
                    detail_dao.get_yahoo_sp_detail(spNo)

                    TransactionId = ""
                    resultFlg = "H"
                    OrderId = []
                    try:
                        row_sp = detail_dao.fetchone()
                        while row_sp:
                            TransactionId = row_sp['TRANSACTIONID']
                            OrderId = row_sp['ORDERID']
                            ShippingSupplierCode = "12"
                            ShippingNumber = shippingNo
   
                            receipt = yh_utils.confirm_home_delivery(TransactionId, OrderId, ShippingSupplierCode, ShippingNumber)
                            logging.info('home_delivery:' + spNo + ' ' + str(receipt))

                            if receipt['Response']['@Status'] == "ok":
                                operations_dao.update_yahoo_sp_detail_home(sp_no=spNo,
                                                                shippingNo=shippingNo,
                                                                OrderId=row_sp['ORDERID'],
                                                                )
                            else:
                                operations_dao.update_yahoo_sp_detail_home_fail(sp_no=spNo,
                                                                OrderId=row_sp['ORDERID'],
                                                                )
                                resultFlg = 'D'

                            row_sp = detail_dao.fetchone()

                        operations_dao.update_yahoo_sp_main_home(sp_no=spNo,
                                                                resultFlg=resultFlg)

                        operations_dao.commit_changes()

                        if resultFlg == 'D':
                            obj = {
                                    "RETURNCODE":"1000", 
                                    "RETURNMSG":"駁回"
                                    }

                        if resultFlg == 'H':
                            obj = {
                                    "RETURNCODE":"1000", 
                                    "RETURNMSG":"完成"
                                    }
       
                    except Exception as e:
                        error, = e.args
                        logging.error(error)
                        #logging.error(error.message)
                        #logging.error(error.context)
                        msg_log = "store_delivery FAIL : %s" % (error.message) 
                        logging.info(msg_log)
                        obj = {
                            "RETURNCODE":"1001", 
                            "RETURNMSG":error.message
                            }
                        return obj
                else:
                    obj = {
                            "RETURNCODE":"1002", 
                            "RETURNMSG":"該筆資料不合法"
                            }
                    return obj

                row = detail_dao.fetchone()
        except Exception as e:
            error, = e.args
          
            logging.error(error)
            msg_log = "store_delivery FAIL : %s" % (error) 
            logging.info(msg_log)
            obj = {
                    "RETURNCODE":"1001", 
                    "RETURNMSG":error
                    }
            return obj

    return obj

@app.get('/store_delivery_cancel')
async def store_delivery_cancel(receiptId: str, request: Request):
        
    if not receiptId:
        raise HTTPException(status_code=401, detail="Missing parameter")

    try:

        receipt = yh_utils.store_delivery_cancel(receiptId)
        logging.info('store_delivery_cancel:' + receiptId + ' ' + str(receipt))


        with YahooApiDao() as operations_dao:
            try:
                if receipt['Response']['@Status'] == "ok":
                    operations_dao.update_yahoo_sp_main_cancel(receiptId=receiptId,
                                                    resultFlg='F',
                                                    )
                    operations_dao.update_yahoo_sp_detail_cancel(receiptId=receiptId,
                                                    resultFlg='F',
                                                    )
                    obj = {
                            "RETURNCODE":"1000", 
                            "RETURNMSG":"完成"
                            }
                else:
                    operations_dao.update_yahoo_sp_main_cancel(receiptId=receiptId,
                                                    resultFlg='D',
                                                    )
                    operations_dao.update_yahoo_sp_detail_cancel(receiptId=receiptId,
                                                    resultFlg='D',
                                                    )
                    obj = {
                            "RETURNCODE":"1002", 
                            "RETURNMSG":"駁回"
                            }

                operations_dao.commit_changes()

            except Exception as e:
                operations_dao.update_yahoo_sp_main_cancel(receiptId=receiptId,
                                                    resultFlg='D',
                                                    )
                operations_dao.update_yahoo_sp_detail_cancel(receiptId=receiptId,
                                                resultFlg='D',
                                                )

                operations_dao.commit_changes()

                obj = {
                        "RETURNCODE":"1003", 
                        "RETURNMSG":"資料傳輸錯誤"
                        }
                error, = e.args
                logging.error(error)
                #logging.error(error.message)
                #logging.error(error.context)
                msg_log = "store_delivery FAIL : %s" % (error.message) 
                logging.info(msg_log)

                return obj

    except Exception as e:
                error, = e.args
                logging.error(error)
                #logging.error(error.message)
                #logging.error(error.context)
                msg_log = "store_delivery FAIL : %s" % (error.message) 
                logging.info(msg_log)
                obj = {
                        "RETURNCODE":"1001", 
                        "RETURNMSG":error.message
                        }
                return obj

    return obj

if __name__ == "__main__":
    settingLog()
    uvicorn.run(app, host="0.0.0.0", port=8018)
