#########################################################################################################################################
# IMPORTS ###############################################################################################################################
import sqlite3
import re
import sys
import time
import parsing
#########################################################################################################################################
# GLOBAL OBJECTS ########################################################################################################################

_affiliation = sys.argv[1];

typ_file = 'mappings/'+'6/'+'/types.txt';
map_file = 'mappings/'+'6/'+'/mapping.txt';
_out_db  = 'representations/'+'6/'+'representations/'+'test.db';
geonames = 'resources/allCountries.db';

TYP = 0;
STR = 1;

_con_in  = sqlite3.connect(geonames);
_cur_in  = _con_in.cursor();
_con_out = sqlite3.connect(_out_db)
_cur_out = _con_out.cursor();

#########################################################################################################################################
# FUNCTIONS #############################################################################################################################

def show(classified,minlen): # Print the output for testing and improvement
    if len(set([label for label,component in classified if label!='address']))>=minlen:
        print('-------------------------------------------------------------------------------------------------------------------\n','>>>',_affiliation);
        print('-------------------------------------------------------------------------------------------------------------------');
        for label,component in classified:
            if label != 'address':
                print(label,':  ',parsing.normalize(component.replace('_',' ').strip(),label));
        print('-------------------------------------------------------------------------------------------------------------------\n');

#########################################################################################################################################
# SCRIPT ################################################################################################################################

components = parsing.get_components(_affiliation);
classified = parsing.classify(components,_cur_in);

show(classified,0);

compos = [el for label_component in [(parsing.normalize(component.replace('_',' ').strip(),label),label,) for label,component in classified if label != 'address'] for el in label_component];
values = tuple([None,None,None,_affiliation]+[compos[i] if i < len(compos) else None for i in range(4*2)]+[None,None,None,None,None,None]);

_cur_out.execute("CREATE TABLE IF NOT EXISTS representations(mentionID TEXT, wos_id TEXT, id INT, string TEXT, c1 TEXT, t1 TEXT, c2 TEXT, t2 TEXT, c3 TEXT, t3 TEXT, c4 TEXT, t4 TEXT, street TEXT, number TEXT, postcode TEXT, city TEXT, country TEXT, concomp INT)");
_cur_out.execute("INSERT INTO representations VALUES("+','.join(('?' for i in range(len(values))))+")",values);
_con_out.commit();
_con_in.close();
_con_out.close();
#########################################################################################################################################
