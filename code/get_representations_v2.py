#########################################################################################################################################
# IMPORTS ###############################################################################################################################
from elasticsearch import Elasticsearch as ES
import sqlite3
import re
import sys
import time
from collections import Counter
import multiprocessing as MP
#########################################################################################################################################
# GLOBAL OBJECTS ########################################################################################################################

mapping = sys.argv[1];
WOS     = sys.argv[2].lower()=='wos';

geonames = 'resources/allCountries.db';
ADR_out  = 'representations/'+mapping+'/representations/'+['bielefeld','wos'][WOS]+'/';
typ_file = 'mappings/'       +mapping+'/types.txt';
map_file = 'mappings/'       +mapping+'/mapping.txt';

gate       = 'svkowos.gesis.intra' if WOS else 'search.gesis.org/es-config/';
addr_index = 'wos' if WOS else 'kb_institutions_bielefeld_addresses';
addr_body  = { "query": {"match_all":{}}, "_source":["_id","addressInformation"] } if WOS else { "query": {"match_all":{}}, "_source":["PK_KB_INST","ADDRESS_FULL","WOS_ID"] };
#addr_body  = { "query": {"match_all":{}}, "_source":["_id","addressInformation"], "slice": {"id":None,"max":None} } if WOS else { "query": {"match_all":{}}, "_source":["PK_KB_INST","ADDRESS_FULL","WOS_ID"], "slice": {"id":None,"max":None} };
client     = ES([gate],scheme='http',port=9200,timeout=60) if WOS else ES([gate],scheme='http',port=80,timeout=60);

_fields_reps = ['mentionID','wos_id','id','string','c1','t1','c2','t2','c3','t3','c4','t4','street','number','postcode','city','country'];

_workers_    = 8 if WOS else 16;
_fullsize_   = 100000000. if WOS else 6500000.;
_scrollsize_ = 10000;
_max_len_    = 4;

TYP = 0; STR = 1; # The below are currently global shared objects, perhaps better to give each process a copy...
_str2type = { re.split(r'\t+',line.rstrip())[0]: re.split(r'\t+',line.rstrip())[1:] for line in open(map_file) };
_level    = { line.split()[0]: int(line.split()[1]) for line in open(typ_file) };
_type2str = dict();
for string in _str2type:
    if len(_str2type[string])==1:
        _str2type[string].append('');
    if _str2type[string][TYP] in _type2str:
        _type2str[_str2type[string][TYP]].append(string);
    else:
        _type2str[_str2type[string][TYP]] = [string];
for label in _type2str:
    _type2str[label] = sorted(_type2str[label],reverse=True); # So that longer matches are found first

streets      = ['Weg','Str','Pl','Platz','Chaussee','Allee','Gasse','Ring','POB','Rd','Road','Strasse','Straße','Street','Way','Damm','Ufer','Postfach','Steig'];
street_regex = '^(?!([A-Z]-)?[0-9])[aA-zZ]{3,}_?('+'|'.join([variant for street in streets for variant in [street,street.lower(),street.upper()]])+')_?([0-9]{1,3}-)?[0-9]{0,3}[aA-hH]?_$';

ADDRESS    = re.compile(r'(,([A-Za-z]| )*('+'|'.join([variant for street in streets for variant in [street,street.lower(),street.upper()]])+')\s*[1-9][0-9]*.*)|(,\s*(D-|W-|O-|DE-)?[0-9]{4,5}.*)');
STREET     = re.compile(street_regex);
POSTCO     = re.compile(r'[0-9]{5}|[0-9]{4}');
NUMBER     = re.compile(r'[0-9]+');
REGEX      = {string:re.compile(r'\b'+string        +r'\b') for string in _str2type};
REGEX_suff = {string:re.compile(      string.lower()+r'_') for string in _str2type};
REGEX_infi = {string:re.compile(      string) for string in _str2type};
COUNTRY    = re.compile(r'Germany_|Ddr|Brd|Fed_Rep_Ger_|Ger_Dem_Rep_');

#########################################################################################################################################
# CLASS DEFINITIONS #####################################################################################################################

class ADR:
    def __init__(self,addr,city_,country_,postcode_,geo_cur):
        self.components = [None for i in range(_max_len_)];
        self.types      = [None for i in range(_max_len_)];
        self.street     = None;
        self.number     = None;
        self.postcode   = None;
        self.city       = None;
        self.country    = None;
        components = get_components(addr);
        classified = classify(components,geo_cur);
        compos     = [(normalize(component.replace('_',' ').strip(),label),label,) for label,component in classified if label != 'address'];
        for i in range(min(_max_len_,len(compos))):
            self.components[i] = compos[i][0];
            self.types[i]      = compos[i][1];
    def show(self):
        for attr in vars(self):
            print(attr, getattr(self,attr));

#########################################################################################################################################
# FUNCTIONS #############################################################################################################################

def lookup(string,cur): # See if the component corresponds to a geographical entity - problem is that almost everything is a city or such
    freq = cur.execute("SELECT COUNT(DISTINCT geonameid) FROM alternatives WHERE alternative=?",(string,)).fetchall()[0][0];
    if freq > 0:
        types = set([row[0] for row in cur.execute("SELECT feature_class FROM geonames WHERE geonameid IN (SELECT DISTINCT geonameid FROM alternatives WHERE alternative=?)",(string,)).fetchall()]);
        if 'A' in types:
            return 'address';
        elif 'P' in types:
            return 'address';
    return None;

def decompose(string): # Find an obvious address suffix in an affiliation string
    match = re.search(ADDRESS,string);
    start = match.start() if match else len(string);
    return string[:start], string[start:];

def clean(labels): # Remove duplicate labels, order needs to be maintained
    seen    = set([]);
    labels_ = [];
    for label in labels:
        if not label in seen:
            labels_.append(label);
            seen.add(label);
    return labels_;

def get_components(string): # Split the affiliation string into components, remove obvious address parts
    string_,address = decompose(string)
    return [component.strip() for component in string_.split(',')];

def classify(components,geo_cur): # Determine possible labels for each component
    components_ = [''.join([(term[0].upper()+term[1:].lower())+'_' if len(term)>1 else term[0].upper()+'_' for term in component.split()]) for component in components];
    labelling   = {component:[] for component in components_};
    for component in components_:
        geo = lookup(component.replace('_',' '.strip()),geo_cur);
        if geo != None:
            labelling[component].append(geo);
        for term in component.split('_'):
            if term in _str2type:
                labelling[component].append(_str2type[term][0]);
        if STREET.match(component):
            labelling[component].append('address');
        if POSTCO.match(component):
            labelling[component].append('address');
    for component in labelling:
        labelling[component] = clean(labelling[component]);
    classified = [(labelling[component][0],component,) if len(labelling[component])==1 else decide(labelling[component],component) if len(labelling[component])>1 else investigate(component,[labelling[component_] for component_ in components_ if len(labelling[component_])>=1]) for component in components_];
    return classified;

def decide(labels,component): # Determine one label if multiple labels are proposed for one component
    if len(labels) > 1 and labels[-1] =='clinic' and _level[labels[-2]] >= _level['clinic'] and ('Klin_' in component or 'Clin_' in component):
        return (labels[-2],component,);
    return (labels[-1],component,);

def investigate(component,all_components): # Determine a label for a component if no label was found so far by looking more closely or defaulting
    if NUMBER.search(component) or STREET.search(component) or POSTCO.search(component) or COUNTRY.search(component):
        return ('address',component,);
    labels = [];
    for keyword in REGEX_suff:
        if REGEX_suff[keyword].search(component):
            labels.append(_str2type[keyword][0]);
    if len(labels) == 0:
        return ('other' if len(all_components)>1 else 'other',component,);
    return decide(labels,component);

def normalize(component,label): # Prepare the output component representation and labelling in the desired way
    if label == 'address':
        return component;
    terms  = component.split();
    for i in range(len(terms)):
        for string in  _type2str[label]:
            if REGEX_infi[string].match(terms[i]):
                terms[i] = _str2type[string][1];
                break;
    return (' '.join([term for term in terms if not term=='' or term==' '])).strip();

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
        rows       = [(addr_obj['full_address'],None,doc['_id'],doc['_id']+'_'+addr_obj['addr_no'],addr_obj['city'],addr_obj['country'],addr_obj['zip']) for doc in page['hits']['hits'] for addr_obj in doc['_source']['addressInformation']['address']] if WOS else [(doc['_source']['ADDRESS_FULL'],int(doc['_source']['PK_KB_INST']),doc['_source']['WOS_ID'],doc['_id'],None,'Germany',None) for doc in page['hits']['hits']];
        objs       = [];
        mentionIDs = [];
        WOS_IDs    = [];
        IDs        = [];
        addrs      = [];
        insts      = [];
        for addr, ID, WOS_ID, mentionID, city, country, postcode in rows:
            obj = ADR(addr,city,country,postcode,cur_in);
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
