#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 22:00:09 2020

@author:
"""

from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta

def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday, 
                      tms.tm_hour, tms.tm_min, tms.tm_sec, 
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))


def extract_email_network(rdd):
    rdd_mail = rdd.map(lambda x:(Parser().parsestr(x)))
    rdd_full_email_tuples = rdd_mail.map(lambda x: (x.get('From'),[x.get('To'),x.get('Cc'),x.get('Bcc')],date_to_dt(x.get('Date'))))
    val_by_vec = lambda x, t, d: [(yield(x,i,d)) for i in t]
    concat_csv_strings = lambda x: (str(x[0])+','+str(x[1])+','+str(x[2])).split(',')
    rdd_email_triples = rdd_full_email_tuples.map(lambda x: (x[0],concat_csv_strings(x[1]),x[2])).flatMap(lambda x: (val_by_vec(x[0],x[1],x[2])))
    email_regex = '[^\s]+@[a-zA-Z0-9_.!#$%&*+-/=?^_`{|}~+-]*(enron.com)+$'
    valid_email = lambda s: True if re.compile(email_regex).fullmatch(s)!=None else False
    not_self_loop = lambda t: True if t[0]!=t[1] else False
    rdd_email_triples_enron = rdd_email_triples.map(lambda x: (x[0],x[1].strip(),x[2]))
    rdd_email_triples_enron = rdd_email_triples_enron.filter(lambda x: valid_email(x[0])).filter(lambda x: valid_email(x[1]))
    rdd_email_triples_enron = rdd_email_triples_enron.filter(lambda x: not_self_loop(x))
    distinct_triples = rdd_email_triples_enron.distinct()
    return distinct_triples


def convert_to_weighted_network(rdd, drange=None):
    if (drange):
        if drange[0]>drange[1]:
            rdd1 = rdd.filter(lambda x:x[2] >= drange[1] and x[2] <= drange[0])
            rdd2 = rdd1.map(lambda x:((x[0],x[1]),1)).reduceByKey(lambda a,b:a+b).map(lambda x:(x[0][0],x[0][1],x[1]))
        else:
            rdd1 = rdd.filter(lambda x:x[2] <= drange[1] and x[2] >= drange[0])
            rdd2 = rdd1.map(lambda x:((x[0],x[1]),1)).reduceByKey(lambda a,b:a+b).map(lambda x:(x[0][0],x[0][1],x[1]))
    else:
        rdd2 = rdd.map(lambda x:((x[0],x[1]),1)).reduceByKey(lambda a,b:a+b).map(lambda x:(x[0][0],x[0][1],x[1]))
    return rdd2


def get_out_degrees(rdd):
    rdd5 = rdd.map(lambda x: (x[0],x[2]))
    rdd61 = rdd.map(lambda x: (x[1],0))
    rdd71 = rdd61.union(rdd5).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1],x[0])).sortBy(lambda x: (x[0],x[1]),False)
    return rdd71

      
def get_in_degrees(rdd):
    rdd6 = rdd.map(lambda x: (x[1],x[2]))
    rdd62 = rdd.map(lambda x: (x[0],0))
    rdd67 = rdd6.union(rdd62).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1],x[0])).sortBy(lambda x: (x[0],x[1]),False)
    return rdd67

          
def get_out_degree_dist(rdd):
    rdd5 = rdd.map(lambda x: (x[0],x[2]))
    rdd61 = rdd.map(lambda x: (x[1],0))
    rdd71 = rdd61.union(rdd5).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1],x[0]))
    rdd5 = rdd71.map(lambda x: (x[0],1)).reduceByKey(lambda x,y:x+y).sortByKey()
    return rdd5


def get_in_degree_dist(rdd):
    rdd6 = rdd.map(lambda x: (x[1],x[2]))
    rdd62 = rdd.map(lambda x: (x[0],0))
    rdd67 = rdd6.union(rdd62).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1],x[0]))
    rdd8 = rdd67.map(lambda x: (x[0],1)).reduceByKey(lambda x,y:x+y).sortByKey()
    return rdd8

