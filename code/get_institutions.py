#########################################################################################################################################
# IMPORTS ###############################################################################################################################
import sqlite3
import re
import sys
from collections import Counter
#########################################################################################################################################
# GLOBAL OBJECTS ########################################################################################################################

mapping = sys.argv[1];
subset  = sys.argv[2];
filenum = sys.argv[3];

geonames = 'resources/allCountries.db';
ADDR_in  = 'representations/'+mapping+'/representations/'+subset+'/'+filenum+'.db';
INST_out = 'representations/'+mapping+'/institutions/'   +subset+'/'+filenum+'.db';
typ_file = 'mappings/'+mapping+'/types.txt';
map_file = 'mappings/'+mapping+'/mapping.txt';
ill_file = 'mappings/'+mapping+'/illegals.txt';
phr_fold = 'phrases/'+mapping+'/'+subset+'/';
cnt_fold = 'counts/'+mapping+'/' +subset+'/';
tra_fold = 'transforms/'+mapping+'/' +subset+'/';

_terms_ = True;

TYP = 0;
STR = 1;

_con_in_  = sqlite3.connect(ADDR_in);
_cur_in_  = _con_in_.cursor();
_con_out_ = sqlite3.connect(INST_out);
_cur_out_ = _con_out_.cursor();

_types    = { line.split()[0]: line.split()[1] for line in open(typ_file) };

_illegals_    = set([line.rstrip() for line in open(ill_file,'r')]);
_phrases_     = { typ: set(    [tuple(line.rstrip().split(' ')) for line in open(phr_fold+typ+'.txt','r')]) for typ in _types };
_counts_      = { typ: Counter([tuple(line.rstrip().split(' ')) for line in open(cnt_fold+typ+'.txt','r')]) for typ in _types };
_transforms_  = { typ: {line.rstrip().split(' ')[0]:line.rstrip().split(' ')[1] for line in open(tra_fold+typ+'.txt','r')}  for typ in _types };

_scrollsize_        = 10000;
_fullsize_          = float(_cur_in_.execute("SELECT count(*) FROM representations").fetchall()[0][0]);
_max_len_           = 4;
_min_component_len_ = 2;

_fields_inst = ['mentionID','wos_id','id','string']+[typ.lower()+str(num) for typ in _types for num in range(1,_max_len_+1)]+['street','number','postcode','city','country'];

#########################################################################################################################################
# CLASS DEFINITIONS #####################################################################################################################

class INST: #TODO: Possible to speed up?
    def __init__(self,postcode,city,country,street,number,components,types):
        self.postcode   = postcode;
        self.city       = city;
        self.country    = country;
        self.street     = street;
        self.number     = number;
        for typ in _types:
            for j in range(1,_max_len_+1):
                setattr(self,str(typ)+str(j),None);
        for i in range(len(components)):
            rep = list(get_rep(components[i],str(types[i])));
            for j in range(len(rep)):
                setattr(self,str(types[i])+str(j+1),rep[j]);
        city_rep = list(get_rep(city,'city'));
        for j in range(len(city_rep)):
            setattr(self,'city'+str(j+1),city_rep[j]);
    def show(self):
        for attr in vars(self):
            print(attr, getattr(self,attr));  

#########################################################################################################################################
# FUNCTIONS #############################################################################################################################

def compress(l):
    return hash(tuple(l));

def ngrams(seq,n):
    return [tuple(seq[i-n:i]) for i in range(n,len(seq)+1) ];

def get_rep(component,typ):
    if not _terms_:
        if component == None: return [];
        return set([component[0].upper()+component[1:].lower().strip()]);
    parts   = [_transforms_[typ][part] if part in _transforms_[typ] else part for part in re.findall(r'\w+',component)] if component != None else [];
    parts   = [part[0].upper()+part[1:].lower() for part in parts];
    bigrams = ngrams(parts,2);
    if len(parts)<=1:
        return parts;
    line   = [];
    phrase = bigrams[0][0];
    for bigram in bigrams:
        if bigram in _phrases_[typ]:
            phrase += '_'+bigram[1];
        else:
            line.append(phrase);
            phrase = bigram[1];
    if len(line)==0 or not line[-1].endswith(phrase): line.append(phrase);
    rep = set([string for string in line if len(string)>=_min_component_len_]) - _illegals_;
    rep = set([tup[1] for tup in sorted([(_counts_[typ][el],el) for el in rep])][:_max_len_]) if len(rep)>_max_len_ else rep;
    rep = set([el[0].upper()+el[1:].lower() for el in rep]);
    return rep;

def main(): #TODO: Change the get_institutions.py such that it uses the replacements on terms
    rows      = _cur_in_.fetchmany(_scrollsize_);
    size      = len(rows);
    page_num  = 0; 
    while (size > 0):
        page_num  += 1;
        objs       = [];
        mentionIDs = [];
        IDs        = [];
        WOS_IDs    = [];
        addrs      = [];
        insts      = [];
        for mentionID,wos_id,inst_id,string,c1,t1,c2,t2,c3,t3,c4,t4,street,number,postcode,city,country,concomp in rows:
            inst = INST(postcode,city,country,street,number,(c1,c2,c3,c4),(t1,t2,t3,t4));
            inst_id = compress([getattr(inst,typ+str(num)) for typ in _types for num in range(1,_max_len_+1)]) if subset=='wos' else inst_id;
            insts.append(inst);
            IDs.append(inst_id);
            WOS_IDs.append(wos_id);
            mentionIDs.append(mentionID);
            addrs.append(string);
        _cur_out_.executemany("INSERT INTO representations("+','.join(_fields_inst)+") VALUES("+', '.join(['?' for i in range(9+len(_types)*_max_len_)])+")",(tuple([mentionIDs[i],WOS_IDs[i],IDs[i],addrs[i]]+[getattr(insts[i],typ+str(num)) for typ in _types for num in range(1,_max_len_+1)]+[insts[i].street,insts[i].number,insts[i].postcode,insts[i].city,insts[i].country]) for i in range(len(insts))));
        _con_out_.commit();
        sys.stdout.write('...roughly '+str(100*page_num*_scrollsize_/_fullsize_)+'% done.'+'\r'); sys.stdout.flush();
        rows = _cur_in_.fetchmany(_scrollsize_);
        size = len(rows);
    #print 'Done with loading addresses. Creating indexes';
    #for column in _fields_inst+['concomp']:
    #    print column;
    #    _cur_out_.execute("CREATE INDEX "+column+"_index on representations("+column+")"); _con_out_.commit();

#########################################################################################################################################
# PREPARING #############################################################################################################################

_cur_out_.execute("DROP TABLE IF EXISTS representations");
_cur_out_.execute("CREATE TABLE representations(mentionID TEXT, wos_id TEXT, id INT, string TEXT, "+', '.join([typ.lower()+str(num)+' TEXT' for typ in _types for num in range(1,_max_len_+1)])+", street TEXT, number TEXT, postcode TEXT, city TEXT, country TEXT, concomp INT)");

#########################################################################################################################################
# LOADING ADDRESSES #####################################################################################################################

_cur_in_.execute("SELECT * FROM representations");
main();
'''
try:
    main();
except KeyboardInterrupt:
    print 'Interrupted. Creating indexes';
    for column in _fields_inst+['concomp']:
        print column;
        _cur_out_.execute("DROP   INDEX IF EXISTS "+column+"_index");
        _cur_out_.execute("CREATE INDEX "+column+"_index on representations("+column+")"); _con_out_.commit();
'''
#########################################################################################################################################
