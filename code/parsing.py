#########################################################################################################################################
# IMPORTS ###############################################################################################################################
import re
import sqlite3
#########################################################################################################################################
# GLOBAL OBJECTS ########################################################################################################################

mapping  = '6'; # This and the below should be overwritten when the programs run that imported this library
geonames = 'resources/allCountries.db';
typ_file = 'mappings/'       +mapping+'/types.txt';
map_file = 'mappings/'       +mapping+'/mapping.txt';

con_in = sqlite3.connect(geonames);
cur_in = con_in.cursor();

TYP = 0; STR = 1;
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
    classified = [];
    for component in components_:
        if len(labelling[component]) == 1:
            classified += [(labelling[component][0],component,)];
        elif len(labelling[component]) > 1:
            classified += decide(labelling[component],component);
        else:
            classified += investigate(component,[labelling[component_] for component_ in components_ if len(labelling[component_])>=1]);
    return classified;

def decide(labels,component): # Determine one label if multiple labels are proposed for one component
    if len(labels) > 1 and labels[-1] =='clinic' and _level[labels[-2]] >= _level['clinic'] and ('Klin_' in component or 'Clin_' in component):
        return (labels[-2],component,);
    #TODO: Implement here a way to detect if there are maybe legitimitely multiple components and return them
    return [(labels[-1],component,)];

def investigate(component,all_components): # Determine a label for a component if no label was found so far by looking more closely or defaulting
    if NUMBER.search(component) or STREET.search(component) or POSTCO.search(component) or COUNTRY.search(component):
        return [('address',component,)];
    labels = [];
    for keyword in REGEX_suff:
        if REGEX_suff[keyword].search(component):
            labels.append(_str2type[keyword][0]);
    if len(labels) == 0:
        return [('other' if len(all_components)>1 else 'other',component,)];
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
