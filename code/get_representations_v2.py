#########################################################################################################################################
# IMPORTS ###############################################################################################################################
from elasticsearch import Elasticsearch as ES
import sqlite3
import re
import sys
import time
from collections import Counter
import multiprocessing as MP
import parsing
#########################################################################################################################################
# GLOBAL OBJECTS ########################################################################################################################

mapping = sys.argv[1];
WOS     = sys.argv[2].lower()=='wos';

ADR_out     = 'representations/'+mapping+'/representations/'+['bielefeld','wos'][WOS]+'/';
geonames    = 'resources/allCountries.db';
#typ_file = 'mappings/'       +mapping+'/types.txt';
#map_file = 'mappings/'       +mapping+'/mapping.txt';

gate       = 'svkowos.gesis.intra' if WOS else 'search.gesis.org/es-config/';
addr_index = 'wos' if WOS else 'kb_institutions_bielefeld_addresses';
addr_body  = { "query": {"match_all":{}}, "_source":["_id","addressInformation"] } if WOS else { "query": {"match_all":{}}, "_source":["PK_KB_INST","ADDRESS_FULL","WOS_ID"] };
client     = ES([gate],scheme='http',port=9200,timeout=60) if WOS else ES([gate],scheme='http',port=80,timeout=60);

_fields_reps = ['mentionID','wos_id','id','string','c1','t1','c2','t2','c3','t3','c4','t4','street','number','postcode','city','country'];

_workers_    = 8 if WOS else 16;
_fullsize_   = 100000000. if WOS else 6500000.;
_scrollsize_ = 10000;
_max_len_    = 8;

#########################################################################################################################################
# CLASS DEFINITIONS #####################################################################################################################

class ADR:
    def __init__(self,addr,city_,country_,postcode_,year,geo_cur):
        self.components = [None for i in range(_max_len_)];
        self.types      = [None for i in range(_max_len_)];
        self.street     = None;
        self.number     = year;
        self.postcode   = None;
        self.city       = None;
        self.country    = None;
        components = parsing.get_components(addr);
        classified = parsing.classify(components,geo_cur);
        compos     = [(parsing.normalize(component.replace('_',' ').strip(),label),label,) for label,component in classified if label != 'address'];
        for i in range(min(_max_len_,len(compos))):
            self.components[i] = compos[i][0];
            self.types[i]      = compos[i][1];
    def show(self):
        for attr in vars(self):
            print(attr, getattr(self,attr));

#########################################################################################################################################
# PREPARING #############################################################################################################################

_cons_in_  = [sqlite3.connect(geonames) for x in range(_workers_)];
_curs_in_  = [con_in.cursor() for con_in in _cons_in_];
_cons_out_ = [sqlite3.connect(ADR_out+str(x)+'.db') for x in range(_workers_)];
_curs_out_ = [con_out.cursor() for con_out in _cons_out_];

for cur_out in _curs_out_:
    cur_out.execute("DROP TABLE IF EXISTS representations");
    cur_out.execute("CREATE TABLE representations(mentionID TEXT, wos_id TEXT, id INT, string TEXT, c1 TEXT, t1 TEXT, c2 TEXT, t2 TEXT, c3 TEXT, t3 TEXT, c4 TEXT, t4 TEXT, street TEXT, number TEXT, postcode TEXT, city TEXT, country TEXT, concomp INT)");

#########################################################################################################################################
# LOADING ADDRESSES #####################################################################################################################

def work(Q,cur_out,con_out,cur_in):
    while True:
        page = None;
        try:
            page = Q.get(timeout=60);
        except:
            print('Could not get any further job from queue within 60s. Stopping work.');
        if page==None: break;
        sid, size  = page['_scroll_id'], len(page['hits']['hits']);
        rows       = [  (   addr_obj['full_address'],
                            None,
                            doc['_id'],
                            doc['_id']+'_'+addr_obj['addr_no'],
                            addr_obj['city'],
                            addr_obj['country'],
                            addr_obj['zip'],
                            doc['_source']['pub_info']['pubyear'] if 'pub_info' in doc['_souce'] and 'pubyear' in doc['_source']['pub_info'] else None
                        )
                            for doc in page['hits']['hits'] for addr_obj in doc['_source']['addressInformation']['address'] ] if WOS else [ ( doc['_source']['ADDRESS_FULL'],
                                                                                                                                              int(doc['_source']['PK_KB_INST']),
                                                                                                                                              doc['_source']['WOS_ID'],
                                                                                                                                              doc['_id'],
                                                                                                                                              None,
                                                                                                                                              'Germany',
                                                                                                                                              None,
                                                                                                                                              None
                                                                                                                                             )
                                                                                                                                               for doc in page['hits']['hits'] ];
        objs       = [];
        mentionIDs = [];
        WOS_IDs    = [];
        IDs        = [];
        addrs      = [];
        insts      = [];
        for addr, ID, WOS_ID, mentionID, city, country, postcode, year in rows:
            obj = ADR(addr,city,country,postcode,year,cur_in);
            objs.append(obj);
            WOS_IDs.append(WOS_ID);
            IDs.append(ID);
            mentionIDs.append(mentionID);
            addrs.append(addr);
        cur_out.executemany("INSERT INTO representations("+','.join(_fields_reps)+") VALUES("+', '.join(['?' for x in range(17)])+")",(tuple([mentionIDs[i],WOS_IDs[i],IDs[i],addrs[i]]+[objs[i].components[0],objs[i].types[0],objs[i].components[1],objs[i].types[1],objs[i].components[2],objs[i].types[2],objs[i].components[3],objs[i].types[3]]+[objs[i].street,objs[i].number,objs[i].postcode,objs[i].city,objs[i].country]) for i in range(len(objs))));
        con_out.commit();

def main():
    Q       = MP.Queue();
    workers = [MP.Process(target=work, args=(Q,_curs_out_[x],_cons_out_[x],_curs_in_[x],)) for x in range(_workers_)];
    for worker in workers: worker.start();
    page     = client.search(index=addr_index,body=addr_body,scroll='2m',size=_scrollsize_);
    sid      = page['_scroll_id'];
    size     = float(page['hits']['total']['value']) if WOS else float(page['hits']['total']);
    returned = size;
    page_num = 0;
    while (returned > 0):
        page_num  += 1;
        page       = client.scroll(scroll_id=sid, scroll='2m'); #TODO: See if there is a way to get multiple scroll slices to force the WoS server to parallel process these requests
        returned   = len(page['hits']['hits']);
        if returned == 0: break;
        while Q.qsize()>1000000/_scrollsize_:
            time.sleep(1);
        Q.put(page);
        sys.stdout.write('...roughly '+str(100*page_num*_scrollsize_/size)+'% done. Queue size: '+str(Q.qsize())+' Page size: '+str(returned)+'-------\r'); sys.stdout.flush();
    for worker in workers: worker.join();
    print('Done with loading addresses.');
try:
    main()
except KeyboardInterrupt:
    print('Interrupted.');

#########################################################################################################################################
