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

app = FastAPI()

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


@app.post('/store_delivery')
async def gettaazeuid(spNo: str, request: Request):
        
    if not spNo:
        raise HTTPException(status_code=401, detail="Missing parameter")

    con = cx_Oracle.connect(User, Pwd, cx_Oracle.makedsn('192.168.100.168', '1521', None, 'taaze'), encoding="UTF-8", nencoding="UTF-8")
    cur = con.cursor()

    sql = """SELECT * FROM APP_YAHOO_SP_MAIN WHERE SP_NO = '%s' """
    cur.execute (sql % (spNo))
    cur.rowfactory = lambda *args: dict(zip([d[0] for d in cur.description], args))
    
    spConfirmFlg = ""
    try:
        row = cur.fetchone()
        while row:
            spConfirmFlg = row['SP_CONFIRM_FLG']
            if spConfirmFlg == 'B':
                sql_sp = """SELECT 
                                SPD.ORDERID AS ORDERID, 
                                OD.TRANSACTIONID AS TRANSACTIONID, 
                                SPD.SHIPPINGNUMBER AS SHIPPINGNUMBER 
                            FROM APP_YAHOO_SP_DETAIL SPD, APP_YAHOO_OD_DETAIL OD WHERE 
                                SPD.ORDERID = OD.ORDERID 
                                AND SPD.SP_NO = '%s' """
                cur_sp = con.cursor()
                cur_sp.execute (sql_sp % (spNo))
                cur_sp.rowfactory = lambda *args: dict(zip([d[0] for d in cur_sp.description], args))

                try:
                    row_sp = cur_sp.fetchone()

                except cx_Oracle.DatabaseError as e:
                    con.rollback()
                    error, = e.args
                    logging.error(error.code)
                    logging.error(error.message)
                    logging.error(error.context)
                    msg_log = "store_delivery FAIL : %s" % (error.message) 
                    logging.info(msg_log)
                    raise HTTPException(status_code=401, detail="Bad store_delivery")

    
            row = cur.fetchone()
    except cx_Oracle.DatabaseError as e:
        con.rollback()
        error, = e.args
        logging.error(error.code)
        logging.error(error.message)
        logging.error(error.context)
        msg_log = "store_delivery FAIL : %s" % (error.message) 
        logging.info(msg_log)
        raise HTTPException(status_code=401, detail="Bad store_delivery")
       
    finally:
        cur.close()
        con.close()

    return ""

settingLog()