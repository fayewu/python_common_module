#!/usr/bin/env python
#-*- coding: utf-8 -*-

import logging
import MySQLdb
import MySQLdb.cursors

class database_cursor(object):
    def __init__(self, host, port, user, passwd, db):
        try:
            self.db_connect = MySQLdb.connect(
                                            host = host, port = port,
                                            user = user, passwd = passwd,
                                            db = db, 
                                            cursorclass = MySQLdb.cursors.SSCursor
                                            )
        except Exception as e:
            logging.error("Database Connection Error" + str(e))
            exit()
        self.cursor = self.db_connect.cursor()

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.cursor.close()
        if isinstance(exc_value, Exception):
            self.db_connect.rollback()
        else:
            self.db_connect.commit()
        self.db_connect.close()

class db_ope(object):
    def __init__(self, host, port, user, passwd, db):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db

    def write(self, sql, esp_value = None):
        with database_cursor(self.host, self.port, self.user, 
                                self.passwd, self.db) as cursor:
            cursor.cursor.execute(sql, esp_value)

    def write_many(self, sql, esp_value):
        with database_cursor(self.host, self.port, self.user, 
                                self.passwd, self.db) as cursor:
            cursor.cursor.executemany(sql, esp_value)

    def read(self, sql):
        with database_cursor(self.host, self.port, self.user, 
                                self.passwd, self.db) as cursor:
            cursor.cursor.execute(sql)
            for each in cursor.cursor:
                yield each
