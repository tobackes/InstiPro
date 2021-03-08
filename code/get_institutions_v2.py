#########################################################################################################################################
# IMPORTS ###############################################################################################################################
import sqlite3
import re
import sys
from collections import Counter
from copy import deepcopy as copy
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
cnt_fold = 'counts/'+mapping+'/' +subset+'/';
tra_file = 'transforms/'+mapping+'/' +subset+'/transforms.txt';
spl_file = 'splittings/'+mapping+'/' +subset+'/splittings.txt';

_terms_ = True;

TYP = 0;
STR = 1;

_con_in_  = sqlite3.connect(ADDR_in);
_cur_in_  = _con_in_.cursor();
_con_out_ = sqlite3.connect(INST_out);
_cur_out_ = _con_out_.cursor();

_types      = { line.split()[0]: line.split()[1] for line in open(typ_file) };
_singletons = set(['city','country','street','number','postco']);

_illegals_   = set([line.rstrip() for line in open(ill_file,'r')]);
_counts_     = { typ: Counter([tuple(line.rstrip().split(' ')) for line in open(cnt_fold+typ+'.txt','r')]) for typ in _types };
_transforms_ = { line.rstrip().split(' ')[0]:line.rstrip().split(' ')[1]  for line in open(tra_file,'r') };
_splittings_ = { line.rstrip().split(' ')[0]:line.rstrip().split(' ')[1:] for line in open(spl_file,'r') };

_scrollsize_        = 10000;
_fullsize_          = float(_cur_in_.execute("SELECT count(*) FROM representations").fetchall()[0][0]);
_max_len_           = 4;
_min_component_len_ = 2;

_fields_inst = ['mentionID','wos_id','id','string']+[typ.lower()+str(num) for typ in _types for num in [range(1,_max_len_+1),['']][typ in _singletons]]+['street','number','postco','observed'];
_field2index = {_fields_inst[i]:i for i in range(len(_fields_inst))};

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
    if typ in _singletons:
        return [component];
    parts   = [_transforms_[part.lower()] if part in _transforms_ else part.lower() for part in re.findall(r'\w+',component)] if component != None else [];
    parts   = [split for part in parts for split in (_splittings_[part] if part in _splittings_ else [part])];
    rep     = set([string for string in parts if len(string)>=_min_component_len_]) - _illegals_;
    rep     = [tup[1] for tup in sorted([(_counts_[typ][el],el) for el in rep])][:_max_len_] if len(rep)>_max_len_ else list(rep)+[None for i in range(_max_len_-len(rep))];
    rep     = [el[0].upper()+el[1:].lower() if el != None else el for el in rep];
    return rep;

def distribute(rows):
    for mentionID,wos_id,inst_id,string,c1,t1,c2,t2,c3,t3,c4,t4,c5,t5,c6,t6,c7,t7,c8,t8,street,number,postco,city,country,concomp in rows:
        out_row      = [None for i in range(len(_fields_inst))];
        out_row[:4]  = mentionID,wos_id,inst_id,string;
        out_row[-4:] = street,number,postco,1;
        this_type  = None;
        component  = '';
        components = sorted([tup for tup in ((t1,c1),(t2,c2),(t3,c3),(t4,c4),(t5,c5),(t6,c6),(t7,c7),(t8,c8)) if not tup[0]==None]);
        for next_type,next_comp in components:
            if this_type == next_type:
                component += ' '+next_comp; # in case one component label is used over multiple components
            else: # new type or last iteration
                if component != '': # first iteration
                    key = this_type+['1',''][this_type in _singletons];
                    if key in _field2index: # If we have some labels in the representations that we do not use as defined in the types
                        start              = _field2index[key];
                        end                = start+[_max_len_,1][this_type in _singletons];
                        out_row[start:end] = get_rep(component,this_type);
                this_type = next_type;
                component = next_comp;
        if component != '': # first iteration
            #out_row[_field2index[this_type+['1',''][this_type in _singletons]]:_field2index[this_type+['1',''][this_type in _singletons]]+[_max_len_,1][this_type in _singletons]] = get_rep(component,this_type);
            key = this_type+['1',''][this_type in _singletons];
            if key in _field2index: # If we have some labels in the representations that we do not use as defined in the types
                start              = _field2index[key];
                end                = start+[_max_len_,1][this_type in _singletons];
                out_row[start:end] = get_rep(component,this_type);
        yield out_row;
        for gen_row in generalize(out_row): # Does NOT contain the out_row itself
            yield gen_row;

def generalize(row): #TODO: Mind that the 4 to -4 depend on the column definition which is subject to change
    rep  = set([(_fields_inst[i][:-1],row[i],) for i in range(len(row)) if row[i] != None and i>=4 and i<len(row)-4]);
    #print('-------------------------------------------\n',rep);
    gens = generalizer(rep);
    for j in range(len(gens)):
        gen     = gens[j];
        row_    = [row[i] if i<4 or i>=len(row)-4 else None for i in range(len(row))];
        row_[0] = row_[0]+'_'+str(j);
        key_    = None;
        num_    = None;
        for key,val in sorted(list(gen)): # To make sure that same keys come after each other
            if key != key_:
                key_ = key;
                num  = 1;
            else:
                num += 1; #TODO: WARNING: The +str(num) is very problematic as we might also have fields that are not added a number. Or should be add 1?
            row_[_field2index[key+str(num)]] = val;
            row_[-1]                = 0;
        #print('-----------------------------\n',gen);
        yield row_;

def generalizer(set_rep):
    d    = dict();
    for level,key,val in ((_types[key],key,val,) for key,val in set_rep if not key=='other'):
        if not level in d:
            d[level] = dict();
        if not key in d[level]:
            d[level][key] = set();
        d[level][key].add(val);
    gens = [];
    cur  = set([]);
    for level in sorted(list(d.keys())):
        cur__ = copy(cur);
        for key in d[level]:
            cur_   = copy(cur);
            cur_  |= set(((key,val,) for val in d[level][key]));
            cur__ |= set(((key,val,) for val in d[level][key]));
            gens.append(cur_);
        if cur__ != cur_:
            gens.append(cur__);
        cur = cur__; #Make sure that for the next more specific level, all more general keys are added. Alternative is cartesian product.
    return gens;

def main():
    page_num = 0; 
    while True:
        page_num += 1;
        rows = _cur_in_.fetchmany(_scrollsize_);
        if len(rows) == 0:
            break;
        query = "INSERT INTO representations("+','.join(_fields_inst)+") VALUES("+', '.join(['?' for i in range(len(_fields_inst))])+")";
        _cur_out_.executemany(query,distribute(rows));
        _con_out_.commit();
        sys.stdout.write('...roughly '+str(100*page_num*_scrollsize_/_fullsize_)+'% done.'+'\r'); sys.stdout.flush();

#########################################################################################################################################
# PREPARING #############################################################################################################################

_cur_out_.execute("DROP TABLE IF EXISTS representations");
_cur_out_.execute("CREATE TABLE representations("+', '.join([typ+' TEXT' for typ in _fields_inst])+')');

#########################################################################################################################################
# LOADING ADDRESSES #####################################################################################################################

_cur_in_.execute("SELECT * FROM representations");
main();

#########################################################################################################################################
