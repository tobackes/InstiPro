#########################################################################################################################################
# IMPORTS ###############################################################################################################################
import sqlite3
import re
import sys
import time
#########################################################################################################################################
# GLOBAL OBJECTS ########################################################################################################################

_affiliation = sys.argv[1];

typ_file = 'mappings/'+'6/'+'/types.txt';
map_file = 'mappings/'+'6/'+'/mapping.txt';
_out_db  = 'representations/'+'6/'+'representations/'+'test.db';
geonames = 'resources/allCountries.db';

TYP = 0;
STR = 1;

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

_con_in  = sqlite3.connect(geonames);
_cur_in  = _con_in.cursor();
_con_out = sqlite3.connect(_out_db)
_cur_out = _con_out.cursor();

streets      = ['Weg','Str','Pl','Platz','Chaussee','Allee','Gasse','Ring','POB','Rd','Road','Strasse','Straße','Street','Way','Damm','Ufer','Postfach','Steig'];
street_regex = '^(?!([A-Z]-)?[0-9])[aA-zZ]{3,}_?('+'|'.join([variant for street in streets for variant in [street,street.lower(),street.upper()]])+')_?([0-9]{1,3}-)?[0-9]{0,3}[aA-hH]?_$';

ADDRESS    = re.compile(r'(,([A-Za-z]| )*('+'|'.join([variant for street in streets for variant in [street,street.lower(),street.upper()]])+')\s*[1-9][0-9]*.*)|(,\s*(D-|W-|O-|DE-)?[0-9]{4,5}.*)');
STREET     = re.compile(street_regex);
POSTCO     = re.compile(r'[0-9]{5}|[0-9]{4}');
NUMBER     = re.compile(r'[0-9]+');
REGEX      = {string:re.compile(r'\b'+string        +r'\b') for string in _str2type};
REGEX_suff = {string:re.compile(      string.lower()+r'_') for string in _str2type if len(string)>=4};
REGEX_infi = {string:re.compile(      string) for string in _str2type};
COUNTRY    = re.compile(r'Germany_|Ddr|Brd|Fed_Rep_Ger_|Ger_Dem_Rep_');

_fields_reps = ['mentionID','wos_id','id','string','c1','t1','c2','t2','c3','t3','c4','t4','street','number','postcode','city','country'];

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

def show(classified,minlen): # Print the output for testing and improvement
    if len(set([label for label,component in classified if label!='address']))>=minlen:
        print('-------------------------------------------------------------------------------------------------------------------\n','>>>',_affiliation);
        print('-------------------------------------------------------------------------------------------------------------------');
        for label,component in classified:
            if label != 'address':
                print(label,':  ',normalize(component.replace('_',' ').strip(),label));
        print('-------------------------------------------------------------------------------------------------------------------\n');

#########################################################################################################################################
# SCRIPT ################################################################################################################################
#t          = time.time();
components = get_components(_affiliation);
classified = classify(components,_cur_in);

show(classified,0);

#print(round(time.time()-t,4));

#compos = [el for label_component in [(normalize(component.replace('_',' ').strip(),label),label,) for label,component in classified if label != 'address'] for el in label_component];
#values = tuple([None,None,None,_affiliation]+[compos[i] if i < len(compos) else None for i in range(4*2)]+[None,None,None,None,None,None]);

#_cur_out.execute("CREATE TABLE IF NOT EXISTS representations(mentionID TEXT, wos_id TEXT, id INT, string TEXT, c1 TEXT, t1 TEXT, c2 TEXT, t2 TEXT, c3 TEXT, t3 TEXT, c4 TEXT, t4 TEXT, street TEXT, number TEXT, postcode TEXT, city TEXT, country TEXT, concomp INT)");
#_cur_out.execute("INSERT INTO representations VALUES("+','.join(('?' for i in range(len(values))))+")",values);
#_con_out.commit();
#_con_in.close();
#_con_out.close();
#########################################################################################################################################
