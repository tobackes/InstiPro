#########################################################################################################################################
# IMPORTS ###############################################################################################################################
from elasticsearch import Elasticsearch as ES
import sqlite3
import re
import sys
import time
from collections import Counter
import multiprocessing as MP
from copy import deepcopy as copy
#########################################################################################################################################
# GLOBAL OBJECTS ########################################################################################################################

mapping = sys.argv[1];

DBs_in  = 'representations/'+mapping+'/representations/'+'bielefeld'+'/';
DBs_out = 'representations/'+mapping+'/representations/'+'bielefeld_year'+'/';

gate       = 'svkowos.gesis.intra';
addr_index = 'wos';
addr_body  = { "query": {"term":{"_id": None}}, "_source":["pub_info"] };

_workers_    = 8;
_scrollsize_ = 100;

#########################################################################################################################################
# FUNCTIONS #############################################################################################################################

def get_year(wos_id,client):
    body                         = copy(addr_body);
    body['query']['term']['_id'] = wos_id;
    result                       = client.search(index=addr_index,body=body);
    years                        = [doc['_source']['pub_info']['pubyear'] if 'pub_info' in doc['_source'] and 'pubyear' in doc['_source']['pub_info'] else None for doc in result['hits']['hits']];
    if len(years) != 1:
        print('WARNING: There are',len(years),'results for',wos_id,'. Skipping...');
        return None;
    return years[0];

#########################################################################################################################################
# PREPARING #############################################################################################################################

_cons_in_  = [sqlite3.connect(DBs_in+str(x)+'.db') for x in range(_workers_)];
_curs_in_  = [con_in.cursor() for con_in in _cons_in_];
_cons_out_ = [sqlite3.connect(DBs_out+str(x)+'.db') for x in range(_workers_)];
_curs_out_ = [con_out.cursor() for con_out in _cons_out_];

for cur_out in _curs_out_:
    cur_out.execute("DROP TABLE IF EXISTS representations");
    cur_out.execute("CREATE TABLE representations(mentionID TEXT, wos_id TEXT, id INT, string TEXT, c1 TEXT, t1 TEXT, c2 TEXT, t2 TEXT, c3 TEXT, t3 TEXT, c4 TEXT, t4 TEXT, street TEXT, number TEXT, postcode TEXT, city TEXT, country TEXT, concomp INT)");

_clients = [ES([gate],scheme='http',port=9200,timeout=60) for x in range(_workers_)];

#########################################################################################################################################
# LOADING ADDRESSES #####################################################################################################################

def work(cur_out,con_out,cur_in,client):
    cur_in.execute("SELECT * FROM representations");
    while True:
        rows = cur_in.fetchmany(_scrollsize_);
        if len(rows) == 0:
            break;
        rows_new = [];
        for mentionID, wos_id, bfd_id, string, c1, t1, c2, t2, c3, t3, c4, t4, street, number, postcode, city, country, concomp in rows:
            number = get_year(wos_id,client);
            rows_new.append((mentionID,wos_id,bfd_id,string,c1,t1,c2,t2,c3,t3,c4,t4,street,number,postcode,city,country,concomp,));
        cur_out.executemany("INSERT INTO representations VALUES("+','.join(['?' for x in range(18)])+")",rows_new);
        con_out.commit();

def main():
    workers = [MP.Process(target=work, args=(_curs_out_[x],_cons_out_[x],_curs_in_[x],_clients[x],)) for x in range(_workers_)];
    for worker in workers: worker.start();
    for worker in workers: worker.join();
    print('Done with adding additional information.');
try:
    main()
except KeyboardInterrupt:
    print('Interrupted.');

#########################################################################################################################################
