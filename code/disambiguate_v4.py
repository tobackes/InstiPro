#IMPORTS--------------------------------------------------------------------------------------------------------------------------------------------------------------------
import os, sys, time, colorsys, heapq, datetime, psutil, sqlite3, json
import itertools as it
import numpy as np
from collections import Counter
from operator import itemgetter
from random import shuffle
from copy import deepcopy as copy
from collections import OrderedDict as OD
from orderedset import OrderedSet as OS
from scipy import __version__
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components
from scipy.sparse.csgraph import minimum_spanning_tree as mst
from scipy.sparse import diags, hstack, vstack, triu, isspmatrix_csr
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#GLOBALS--------------------------------------------------------------------------------------------------------------------------------------------------------------------
OBS = 0; CAR = 1; SPE = 2; GEN = 3; REP = 4; STR = 5; MOD = 6; RID = 7; SP_ = 8; TYP = 9; PTS = 10;CLU = 0; MER = 1; DIS = 2; SIM = 1;

MEM = [0,0,0]; TIME = [0,0,0]; SIZE = 0; TIME_ = [0,0]; MERGE = 0; CLUS = 0; COMP = np.array([1]); BOUND = 0; MAXF1 = 0.; MAXPR = [0.,0.];

_key                     = sys.argv[1] if sys.argv[1] != 'None' else None;
_value                   = sys.argv[2] if sys.argv[2] != 'None' else None;
_job_id                  = sys.argv[3];
_cfg_file                = sys.argv[4];
_slot_merge              = True  if len(sys.argv)<=5 or sys.argv[5].lower()=='true'  else False;
_repsize_thr             = 999.  if len(sys.argv)<=6                                 else float(sys.argv[6]);
_allow_complete_merges   = True  if len(sys.argv)<=7 or sys.argv[7].lower()=='true'  else False;  # -nodes +sizes # If true merge also nodes with more general 'slot' set in representations
_min_self_prob           = 0.0   if len(sys.argv)<=8                                 else float(sys.argv[8]);#float(sys.argv[6]);                              # +nodes -sizes # Merge only nodes where the generalization's self prob is at least ...
_weight_self             = False if len(sys.argv)<=9 or sys.argv[9].lower()=='false' else True;#True if sys.argv[7].lower()=='true' else False;  # depends on iteration
_d_                      = 1.0   if len(sys.argv)<=10                                else float(sys.argv[10]);#float(sys.argv[8]);#float(       sys.argv[6]);

_job_id = _job_id + '_' + str(_allow_complete_merges)[0] + str(_min_self_prob).split('.')[1] + str(_weight_self)[0] + str(_d_).split('.')[0];

_p_new_      = False;#bool(int(    sys.argv[5]));
_random_     = False;#bool(int(    sys.argv[7]));
_nbrdm_      = False;#True if      sys.argv[8]=='1' else False;
_top_k_      = None;#None if      sys.argv[9]=='0' else int(sys.argv[7]);
_dbscan_     = False;#bool(int(    sys.argv[10]));
_similarity_ = 'probsim';#'probsim' if sys.argv[11]=='0' else 'cosim';

_clean_all_nodes         = True;  # All unobserved nodes are removed and their specifications split up
_find_edges_gen          = False;
#_slot_merge              = False;  # -nodes +sizes # If true merge only nodes with same         'slot' set in representations
_clean_without_exception = True;  # -nodes -sizes # Clean all unobserved intermediate nodes

_licensing = False;

_oversize                = 25;

_contextfile = 'context.json';

cfg_in = open(_cfg_file,'r'); _cfg = json.loads(cfg_in.read()); cfg_in.close();

_result_db = _cfg['out_dir']+_cfg['result_dir']+_job_id+'.db';

_excluded   = set([]) if _cfg['name_db'].endswith('dfg.db') else set(['None']);
_special    = set([]);#set(['city']);
_typeonly   = False;
_colored    = False;
_checker_   = False; # <---------------------------------------TODO:WARNING !!! This might not be wanted !!!
_max_len_   = 6;

_fields_     = [typ for typ in [line.rstrip().split()[0] for line in open(_cfg['typ_file'])] if not typ in _excluded];
_levels_     = {typ:int(level) for typ,level in [line.rstrip().split() for line in open(_cfg['typ_file'])] if not typ in _excluded};
_field2index = {_fields_[i]:i for i in range(len(_fields_))};
_ftypes_     = {'affiliations':.2,'categories':.18,'coauthornames':.2,'emails':.1,'keywords':.1,'refauthornames':.12,'terms':.15,'years':.02};

_old_scipy_ = int(__version__.split('.')[0])==0;
_p_         = psutil.Process(os.getpid()); _mb_ = 1024*1024;

_feat_db = _cfg['root_dir']+_cfg['feat_dir']+str(_key)+'.db';
_sums_db = _feat_db if _cfg['sums_db'] == None else _cfg['sums_db'];

_skipvals = set(['Res','Sci','Sch','Inst','Dept','Wissensch'])

#TODO: Put the below into the type file
_short_form = { 'university'  : 'UNI',
                'academy'     : 'ACAD',
                'agency'      : 'AGCY',
                'association' : 'ASSOC',
                'factory'     : 'FACT',
                'college'     : 'SCHOOL',
                'clinic'      : 'CLI',
                'company'     : 'CORP',
                'lab'         : 'LAB',
                'collection'  : 'COLL',
                'faculty'     : 'FAC',
                'center'      : 'CTR',
                'site'        : 'SITE',
                'other'       : 'OTHER',
                'field'       : 'FIELD',
                'institute'   : 'INST',
                'subfield'    : 'SUBFIELD',
                'subject'     : 'SUBJ',
                'community'   : 'GRP',
                'chair'       : 'CHAIR',
                'address'     : 'ADDR'}
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#-CLASSES---------------------------------------------------------------------------------------------------------------------------------------------------------------------
class DATA:

    def __init__(self,nodes,mentions,rIDs,make_match,aggregate):
        #--------------------------------------------------------------------------------------------
        print('Initializing...'); t = time.time(); # Creating central mappings and rows+cols
        #--------------------------------------------------------------------------------------------
        index2node           = list(nodes.keys());
        node2index           = {index2node[i]:i for i in range(len(index2node))};
        index2rID            = rIDs;
        rID2index            = {index2rID[i]:i for i in range(len(index2rID))};
        obs, N               = [], [];
        rows_edge, cols_edge = [], [];
        for gen_str in nodes:
            obs.append(nodes[gen_str][OBS]);
            N.append(nodes[gen_str][CAR]);
            node_index            = node2index[gen_str];
            for spe_str in nodes[gen_str][SPE]|set([gen_str]):
                rows_edge.append(node_index);
                cols_edge.append(node2index[spe_str]);
        #--------------------------------------------------------------------------------------------
        print(time.time()-t, 'seconds for 1.', _p_.memory_info().rss/_mb_, 'MB used'); t = time.time(); # Matching information if required
        #--------------------------------------------------------------------------------------------
        rows_match, cols_match = [], [];
        if make_match:
            for i in range(len(list(nodes.keys()))):
                for j in range(len(list(nodes.keys()))):
                    str1 = list(nodes.keys())[i]; str2 = list(nodes.keys())[j];
                    if match([nodes[str1][REP],nodes[str2][REP]]):
                        rows_match.append(node2index[str1]);
                        cols_match.append(node2index[str2]);
        #--------------------------------------------------------------------------------------------
        print(time.time()-t, 'seconds for 2.', _p_.memory_info().rss/_mb_, 'MB used'); t = time.time(); # Creating more mappings
        #--------------------------------------------------------------------------------------------
        mention2node    = [];
        node2mentions   = [[]   for node    in nodes   ];
        rID2nodes       = [[]   for rID     in rIDs    ];
        node2rIDs       = [[]   for node    in nodes   ];
        rID2mentions    = [[]   for rID     in rIDs    ];
        mention2rID     = [None for mention in mentions];
        index2mentionID = [];
        mentionID2index = dict();
        for mention_index in range(len(mentions)):
            node_index = node2index[string(mentions[mention_index][0])];
            rID        = mentions[mention_index][1];
            mentionID  = mentions[mention_index][3];
            rID_index  = rID2index[rID];
            mention2node.append(node_index);
            node2mentions[node_index].append(mention_index);
            rID2nodes[rID_index].append(node_index);
            rID2mentions[rID_index].append(mention_index);
            node2rIDs[node_index].append(rID_index);
            mention2rID[mention_index] = rID_index;
            mentionID2index[mentionID] = len(index2mentionID);
            index2mentionID.append(mentionID);
        #--------------------------------------------------------------------------------------------
        print(time.time()-t, 'seconds for 3.', _p_.memory_info().rss/_mb_, 'MB used'); t = time.time(); # More rows+cols
        #--------------------------------------------------------------------------------------------
        rows_NM, cols_NM     = list(zip(*[[node_index,mention_index] for node_index in range(len(node2mentions)) for mention_index in node2mentions[node_index]]));
        rows_MR, cols_MR     = list(zip(*[[mention_index,mention2rID[mention_index]] for mention_index in range(len(mention2rID))]));
        rows_spec, cols_spec = list(zip(*[[node2index[gen_str],node2index[spe_str]] for gen_str in nodes for spe_str in nodes[gen_str][SP_]|set([gen_str])]));
        #--------------------------------------------------------------------------------------------
        print(time.time()-t, 'seconds for 4.', _p_.memory_info().rss/_mb_, 'MB used'); t = time.time(); # Replace list of mentions by length
        #--------------------------------------------------------------------------------------------
        if not aggregate:
            for key, val in list(nodes.items()):
                nodes[key][RID] = Counter({rid:len(val[RID][rid]) for rid in val[RID]});
        #--------------------------------------------------------------------------------------------
        print(time.time()-t, 'seconds for 5.', _p_.memory_info().rss/_mb_, 'MB used'); t = time.time();
        #--------------------------------------------------------------------------------------------
        self.nodes           = nodes;
        self.index2node      = index2node;
        self.node2index      = node2index;
        self.index2rID       = index2rID;
        self.rID2index       = rID2index;
        self.index2mentionID = index2mentionID;
        self.mentionID2index = mentionID2index;
        self.edge            = csr((np.ones(len(rows_edge)),(rows_edge,cols_edge)),shape=(len(self.nodes),len(self.nodes)), dtype=bool);
        self.obs             = csr(np.array(obs).reshape(len(obs),1),shape=(len(obs),1),dtype=float); #TODO: Should this be defined twice?
        self.car             = csr(np.array(N).reshape(len(N),1),shape=(len(N),1),dtype=float);
        self.obs_            = copy(self.obs);
        self.car_            = copy(self.car);
        self.match           = csr((np.ones(len(rows_match)),(rows_match,cols_match)),shape=(len(self.nodes),len(self.nodes)), dtype=bool);
        self.NM              = csr((np.ones(len(rows_NM)),(rows_NM,cols_NM)),shape=(len(self.nodes),len(mentions)), dtype=bool);
        self.MR              = csr((np.ones(len(rows_MR)),(rows_MR,cols_MR)),shape=(len(mentions),len(rIDs)), dtype=bool);
        self.ment            = csr([[mention[2]] for mention in mentions]);
        self.spec            = csr((np.ones(len(rows_spec)),(rows_spec,cols_spec)),shape=(len(self.nodes),len(self.nodes)), dtype=bool);
        #--------------------------------------------------------------------------------------------
        print(time.time()-t, 'seconds for 6.', _p_.memory_info().rss/_mb_, 'MB used'); t = time.time();
        #--------------------------------------------------------------------------------------------
        self.obs             = self.NM.dot(self.ment);
        self.MR_             = csr(self.ment).multiply(self.MR);
        self.core            = np.zeros(self.ment.shape[0],dtype=bool);
        self.arrow           = diags(np.ones(self.ment.shape[0],dtype=int),0,dtype=bool);
        self.labels          = np.arange(self.car.shape[0]); # initially each node is one cluster
        self.labelling       = self.NM.T.nonzero()[1];
        self.n               = len(self.labels);
        self.MC              = csr((np.ones(len(self.labelling),dtype=bool),(np.arange(len(self.labelling)),self.labelling)),shape=(len(self.labelling),len(self.labels)),dtype=bool);
        self.NC              = self.NM.dot(self.MC);
        self.rids_c          = self.MC.T.dot(self.MR_);
        self.rids_b          = self.NM.dot(self.MR_);
        self.new             = np.ones(self.car.shape[0],dtype=bool);
        self.weight          = self.car.T.multiply(self.edge).multiply(csr(1./self.car.toarray()));
        if _old_scipy_:
            self.weight.setdiag(self.obs.toarray()/self.car.toarray()); #old scipy version
        else:
            self.weight.setdiag(np.ravel(self.obs/self.car));    #new scipy version
        #--------------------------------------------------------------------------------------------
        print(time.time()-t, 'seconds for 7.', _p_.memory_info().rss/_mb_, 'MB used'); t = time.time();
        #--------------------------------------------------------------------------------------------
        #self.index2feat = {ftype: get_index2feat(ftype,mentionID2index,_feat_db) for ftype in _ftypes_};
        #self.feat2index = {ftype: {self.index2feat[ftype][i]:i for i in xrange(len(self.index2feat[ftype]))} for ftype in _ftypes_};
        #--------------------------------------------------------------------------------------------
        print(time.time()-t, 'seconds for 8.', _p_.memory_info().rss/_mb_, 'MB used'); t = time.time();
        #--------------------------------------------------------------------------------------------
        #self.MF         = {ftype: get_MF(ftype,self.mentionID2index,self.feat2index[ftype],_feat_db) for ftype in _ftypes_};
        #--------------------------------------------------------------------------------------------
        print(time.time()-t, 'seconds for 9.', _p_.memory_info().rss/_mb_, 'MB used'); t = time.time();
        #--------------------------------------------------------------------------------------------
        #self.f          = {ftype: get_f(ftype,self.index2feat[ftype],_sums_db) for ftype in _ftypes_};
        #self.f          = {ftype: np.ravel(self.MF[ftype].sum(0))                 for ftype in _ftypes_}; #TODO: Normalization check
        #--------------------------------------------------------------------------------------------
        print(time.time()-t, 'seconds for 10.', _p_.memory_info().rss/_mb_, 'MB used'); t = time.time();
        #--------------------------------------------------------------------------------------------
        #self.one_by_f   = {ftype: 1./self.f[ftype] for ftype in _ftypes_};
        #--------------------------------------------------------------------------------------------
        print(time.time()-t, 'seconds for 11.', _p_.memory_info().rss/_mb_, 'MB used'); t = time.time();
        #--------------------------------------------------------------------------------------------
        #self.freq_x     = {ftype: np.array(self.MF[ftype].sum(1).T) for ftype in _ftypes_};
        #--------------------------------------------------------------------------------------------
        print(time.time()-t, 'seconds for 12.', _p_.memory_info().rss/_mb_, 'MB used'); t = time.time();
        #--------------------------------------------------------------------------------------------

    def update_index(self,keep,r,changed):
        self.index2node         = list(itemgetter(*(keep+[keep[-1]]))(self.index2node))[:-1] if keep != [] else [];
        self.node2index         = {self.index2node[i]:i for i in range(len(self.index2node))};
        self.new                = self.new[keep];
        self.new[keep.index(r)] = changed;
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#FUNCTIONS------------------------------------------------------------------------------------------------------------------------------------------------------------------

#-FUNCTIONS-DRAWING--------------------------------------------------------------------------------------------------------------------

def get_colors(n):
    hsv = [(x/float(n+1), 0.7, 0.999999) for x in range(n+2)];
    shuffle(hsv);
    return hsv;

def color_string(R_,colors): #R_ is sparse boolean row vector with nonzero count for each rID present
    if not _colored:
        return '0.0 0.0 1.0;1.0'
    denom  = R_[0,_cfg['no_none']:].sum(); #index 0 is for None
    string = ':'.join([' '.join([str(num) for num in colors[i]])+';'+str(round(R_[0,i]/denom,4)) for i in R_[0,_cfg['no_none']:].nonzero()[1]]); #index 0 is for None
    return string;

def get_nodes_edges_(edges_in,index2node,D,colors):
    edges    = [];
    nodestrs = [];
    str2dis  = dict();
    for i in range(len(index2node)):
        i_                       = D.node2index[index2node[i]];
        color_str                = color_string(D.rids_b[i_],colors);
        node_display             = '"'+str(D.obs_[i_,0])+' ('+str(round(D.obs[i_,0],1))+')  |  '+str(D.car_[i_,0])+' ('+str(round(D.car[i_,0],1))+')\n'+D.nodes[index2node[i]][STR]+'"';
        str2dis[index2node[i]] = '"'+index2node[i]+'"';
        if D.rids_b[i_].sum() != 0:
            nodestrs.append('"'+index2node[i]+'" [label='+node_display+' style=striped fillcolor="'+color_str+'"]');
        else:
            nodestrs.append('"'+index2node[i]+'" [label='+node_display+' style="filled" fillcolor="gray"]');
        for j in edges_in[i,:].nonzero()[1]:
            j_                = D.node2index[index2node[j]];
            child_display     = '"'+str(round(D.nodes[index2node[j]][OBS]))+' ('+str(round(D.obs[j_,0],1))+')  |  '+str(round(D.nodes[index2node[j]][CAR]))+' ('+str(round(D.car[j_,0],1))+')\n'+D.nodes[index2node[j]][STR]+'"';
            can_be, should_be = can_be_merged(i_,j_,D), should_be_merged(i_,j_,D);
            edge_color        = 'blue' if can_be and not should_be else 'green' if can_be and should_be else 'black';
            edges.append('"'+index2node[j]+'" -> "'+index2node[i]+'" [label="'+str(round(D.weight[i_,j_],2)).strip('0')+'" dir="back" color="'+edge_color+'"]');
    return nodestrs, edges, str2dis;

def get_nodes_edges(edges_in,index2node,D,colors):
    edges    = [];
    nodestrs = [];
    str2dis  = dict();
    for i in range(len(index2node)):
        i_                       = D.node2index[index2node[i]];
        color_str                = color_string(D.rids_b[i_],colors);
        #node_display             = '"'+str(D.obs_[i_,0])+' ('+str(round(D.obs[i_,0],1))+')  |  '+str(D.car_[i_,0])+' ('+str(round(D.car[i_,0],1))+')\n'+D.nodes[index2node[i]][STR]+'"';
        node_display             = '"'+str(int(D.obs_[i_,0]))+'  |  '+str(int(D.car_[i_,0]))+'\n'+string(D.nodes[index2node[i]][REP])+'"';
        str2dis[index2node[i]] = '"'+str(i)+'"';
        if edges_in[i,:].sum() > 0 or edges_in[:,i].sum() > 0: #Only return nodes that have at least one edge (including to itself)
            if D.rids_b[i_].sum() != 0:
                nodestrs.append(str(i)+' [label='+node_display+' style=striped fillcolor="'+color_str+'"]');
            else:
                nodestrs.append(str(i)+' [label='+node_display+' style="filled" fillcolor="gray"]');
        for j in edges_in[i,:].nonzero()[1]:
            j_                = D.node2index[index2node[j]];
            #child_display     = '"'+str(round(D.nodes[index2node[j]][OBS]))+' ('+str(round(D.obs[j_,0],1))+')  |  '+str(round(D.nodes[index2node[j]][CAR]))+' ('+str(round(D.car[j_,0],1))+')\n'+D.nodes[index2node[j]][STR]+'"';
            child_display     = '"'+str(int(D.nodes[index2node[j]][OBS]))+'  |  '+str(int(D.nodes[index2node[j]][CAR]))+'\n'+string(D.nodes[index2node[j]][REP])+'"';
            can_be, should_be = can_be_merged(i_,j_,D), should_be_merged(i_,j_,D);
            edge_color        = 'blue' if can_be and not should_be else 'green' if can_be and should_be else 'black';
            edges.append(str(j)+' -> '+str(i)+' [label="'+str(round(D.weight[i_,j_],2)).strip('0')+'" dir="back" color="'+edge_color+'"]');
    return nodestrs, edges, str2dis;

def repID2Index(repID,cur):
    return cur.execute("SELECT repIDIndex FROM index2repID WHERE repID=?",(repID,)).fetchall()[0][0];

def mentionID2Index(mentionID,cur):
    return cur.execute("SELECT mentionIDIndex FROM index2mentionID WHERE mentionID=?",(mentionID,)).fetchall()[0][0];

def equiDB(D,I=None): #TODO: Check the memory consumption here and see if it can be improved
    OUT = _cfg['out_dir']+'equiDBs/'+str(I)+'/'+_job_id+'.db' if I != None else 'generalizations.db';
    con = sqlite3.connect(OUT); cur = con.cursor();
    cur.execute("CREATE TABLE IF NOT EXISTS generalizations(mentionIDIndex INT, repIDIndex INT, level INT, UNIQUE(mentionIDIndex,repIDIndex))");
    cur.execute("CREATE TABLE IF NOT EXISTS index2mentionID(mentionIDIndex INTEGER PRIMARY KEY AUTOINCREMENT, mentionID TEXT UNIQUE)");
    cur.execute("CREATE TABLE IF NOT EXISTS index2repID(    repIDIndex     INTEGER PRIMARY KEY AUTOINCREMENT, repID     TEXT UNIQUE)");
    cur.execute("CREATE INDEX IF NOT EXISTS mentionIDIndex_index ON generalizations(mentionIDIndex)");
    cur.execute("CREATE INDEX IF NOT EXISTS repIDIndex_index ON generalizations(repIDIndex)");
    cur.execute("CREATE INDEX IF NOT EXISTS level_index ON generalizations(level)");
    cur.execute("CREATE INDEX IF NOT EXISTS mentionID_index ON index2mentionID(mentionID)");
    cur.execute("CREATE INDEX IF NOT EXISTS repID_index     ON index2repID(    repID    )");
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    #TODO: The below is the source of the memory problem:
    #TODO: Did I consider that Nodes is actually changing as it seems to be the same object as D.nodes?
    gen = [(D.index2mentionID[mentionIndex],repID,0,) for nodeIndex,mentionIndex in zip(*D.NM.nonzero()) for repID in Nodes[D.index2node[nodeIndex]][PTS]];
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    edges  = transitive_reduction(set_diagonal(D.edge,csr(np.zeros(D.edge.shape[0],dtype=bool)[:,None]))).T;
    num, i = 1,1;
    while num > 0 and i <= len(_fields_)*_max_len_+1:
        print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
        new  = edges**i;
        gen += [(D.index2mentionID[mentionIndex],repID,i,) for mentionIndex,nodeIndex in zip(*D.NM.T.dot(new).nonzero()) for repID in Nodes[D.index2node[nodeIndex]][PTS]];
        num  = len(new.nonzero()[0]);
        print('...',i,':',num);
        i  += 1;
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    mentionIDs,repIDs = set([row[0] for row in gen]), set([row[1] for row in gen]);
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    cur.executemany("INSERT OR IGNORE INTO index2mentionID(mentionID) VALUES(?)",((mentionID,) for mentionID in mentionIDs));
    cur.executemany("INSERT OR IGNORE INTO index2repID(    repID)     VALUES(?)",((repID,    ) for repID     in repIDs    ));
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    indexOfmentionID = {mentionID:mentionID2Index(mentionID,cur) for mentionID in mentionIDs};
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    indexOfrepID     = {repID    :repID2Index(    repID    ,cur) for repID     in repIDs    };
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    #TODO: The below gets much larger the larger is gen:
    #TODO: Probably the equiDB representation is just too large by itself as lev should not contain unnecessary information
    lev = dict();
    for row in gen:
        pair = tuple(row[:2]); #getting the min level for equal pairs
        if pair not in lev or (pair in lev and lev[pair] > row[-1]):
            lev[pair] = row[-1];
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    print('inserting...');
    cur.executemany("INSERT OR IGNORE INTO generalizations VALUES(?,?,?)",((indexOfmentionID[mentionID],indexOfrepID[repID],lev[(mentionID,repID,)],) for mentionID,repID in lev)); #TODO: Upsert should be much faster with latest slite3 version
    cur.executemany("UPDATE generalizations SET level=MIN(level,?) WHERE mentionIDIndex=? AND repIDIndex=?",((lev[(mentionID,repID,)],indexOfmentionID[mentionID],indexOfrepID[repID],) for mentionID,repID in lev));
    con.commit(); con.close();

def draw(D,colors,I=None,TREE=False):
    #print 'doing transitive reduction...';
    #D.edge = transitive_reduction(D.edge); #TODO: Should this not better be put elsewhere?
    print('start drawing...');
    edges_, index2node = D.edge, D.index2node;
    if TREE:
        edge_    = set_diagonal(D.edge,csr(np.zeros(D.edge.shape[0]))); #Make irreflexive to get DAG
        weights_ = get_view(D.weight,edge_);   # Select only the weights for which there are edges in the reduced edge_
        tree     = max_span_tree(weights_);     # Make a tree from the DAG
        tree     = get_view(edge_,tree);        # Could also convert tree to boolean
        edges_   = tree;
        #edges_, index2node = redundant_tree(D.edge,D.index2node); print edges_.shape, len(index2node)
    #OUT = open(_cfg['viz_file']+['.graph','.tree'][TREE]+'.'+str(I),'w') if I != None else open(_cfg['viz_file'],'w');
    OUT = open(_cfg['out_dir']+'graphs/'+str(I)+'_'+_job_id+['.graph','.tree'][TREE]+'.dot','w') if I != None else open(_cfg['viz_file'],'w');
    OUT.write('digraph G {\nranksep=.3\nnodesep=.2\nnode [shape=box]\n');
    nodestrs, edges, str2dis = get_nodes_edges(edges_,index2node,D,colors);
    for edge in edges:
        OUT.write(edge+'\n');
    for nodestr in nodestrs:
        OUT.write(nodestr+'\n');
    #prec, rec, f1 = prec_rec_f1_([D.nodes[i][RID] for i in D.node2index]);
    bPrec, bRec, bF1 = prec_rec_f1(D.rids_b[:,:]);
    dPrec, dRec, dF1 = prec_rec_f1(D.rids_c[:,:]);
    print('bPrec:',bPrec,'bRec:',bRec,'bF1:',bF1);
    print('dPrec:',dPrec,'dRec:',dRec,'dF1:',dF1);
    #OUT.write('"bPrec:  '+str(round(bPrec,2))+'\n\nbRec:  '+str(round(bRec,2))+'\n\nbF1:  '+str(round(bF1,2))+'\n\ndPrec:  '+str(round(dPrec,2))+'\n\ndRec:  '+str(round(dRec,2))+'\n\ndF1:  '+str(round(dF1,2)) +'" [style=filled fillcolor=grey fontsize=18]\n');
    if False:#not TREE:
        nodes_by_level = get_nodes_by_lat_level(D.nodes);
        for level in nodes_by_level:
            #print level;
            OUT.write('{rank=same; ');
            for node_str in nodes_by_level[level]:
                if node_str in str2dis:
                    #print node_str; print '---------------------------------------';
                    OUT.write(str2dis[node_str]+'; ');
            OUT.write('}');
    OUT.write('}');
    print('done drawing.');
    OUT.close();

def draw_all_contexts(D,colors,I=None,TREE=False):
    edges_, index2node = D.edge, D.index2node;
    downstream         = edges_**20; # Assuming there are no longer paths than 20
    upstream           = edges_.T**20;
    for node_index in range(len(D.index2node)):
        draw_context(D,colors,node_index,downstream,upstream,I,TREE);

def draw_one_context(D,colors,node_index,I=None,TREE=False):
    edges_, index2node = D.edge, D.index2node;
    downstream         = edges_**20; # Assuming there are no longer paths than 20
    upstream           = edges_.T**20;
    draw_context(D,colors,node_index,downstream,upstream,I,TREE);

def draw_context(D,colors,node_index,downstream,upstream,I=None,TREE=False):
    edges_, index2node = D.edge, D.index2node;
    if True:
        downstream_ = downstream[node_index];
        upstream_   = upstream[node_index];
        edges_down  = edges_.multiply(downstream_).multiply(downstream_.T);
        edges_up    = edges_.multiply(upstream_  ).multiply(upstream_.T  );
        edges__     = edges_down + edges_up;
        if TREE:
            edge_    = set_diagonal(edges__,csr(np.zeros(D.edge.shape[0])));    # Make irreflexive to get DAG
            weights_ = get_view(D.weight,edge_);                                # Select only the weights for which there are edges in the reduced edge_
            tree     = max_span_tree(weights_);                                 # Make a tree from the DAG
            tree     = get_view(edge_,tree);                                    # Could also convert tree to boolean
            edges__  = tree;
        filename = index2node[node_index].replace('\n','___').replace(' ','').replace(':','__').replace(',','_').replace('{','').replace('}','');
        OUT      = open(_cfg['out_dir']+'graphs/'+str(I)+'_'+_job_id+['.graph','.tree'][TREE]+'.'+filename+'.dot','w') if I != None else open(_cfg['viz_file'],'w');
        OUT.write('digraph G {\nranksep=.3\nnodesep=.2\nnode [shape=box]\n');
        nodestrs, edges, str2dis = get_nodes_edges(edges__,index2node,D,colors);
        for edge in edges:
            OUT.write(edge+'\n');
        for nodestr in nodestrs:
            OUT.write(nodestr+'\n');
        OUT.write(str(node_index)+' [style=rounded, color=red, fontcolor=red]\n');
        OUT.write('}');
        OUT.close();

def mentioninfos(nodeIndex,D,cur):
    mentionIDs   = [D.index2mentionID[i] for i in D.NM[nodeIndex,:].nonzero()[1]];
    features     = ['mentionID','wos_id','string','street','number','postcode','city','country'];
    query        = "SELECT "+','.join(features)+" FROM representations WHERE mentionID=?";
    mentionInfos = [cur.execute(query,(mentionID,)).fetchall()[0] for mentionID in mentionIDs];
    M = dict();
    for mentionInfo in mentionInfos:
        M[mentionInfo[0]] = {"@type":          "Mention",
                             "@id":            mentionInfo[0],
                             "wos_id":         mentionInfo[1],
                             "address_string": mentionInfo[2],
                             "address":        { "@type":    "Address",
                                                 "@id":      mentionInfo[2],
                                                 "country":  mentionInfo[7],
                                                 "postcode": mentionInfo[5],
                                                 "city":     mentionInfo[6],
                                                 "street":   mentionInfo[3],
                                                 "number":   mentionInfo[4]
                                               },
                            }
    return M;

def nodeinfos(nodeIndex,tree_spec,D):
    node        = D.nodes[D.index2node[0]];
    obs_,car_   = D.NM[nodeIndex,:].sum(),tree_spec[nodeIndex,:].sum();
    components  = dict();
    for component,value in D.nodes[D.index2node[nodeIndex]][REP]:
        if component+'_tags' in components:
            components[component+'_tags'].append(value);
        else:
            components[component+'_tags'] = [value];
    return components,obs_,car_;

def make_rep_str_(child_ind,parent_ind): #TODO: include a sorting of the components
    new_string = D.nodes[D.index2node[child_ind]][STR];
    components = [comp.split(':') for comp in new_string.split('\n')];
    components = [(feat,val[2:-1].split(','),) for feat,val in components] if not components==[['']] else [];
    new_string = ','.join([feat+':'+('+'.join(values)) for feat,values in components]);
    wen_string = D.nodes[D.index2node[parent_ind]][STR];
    components = [comp.split(':') for comp in wen_string.split('\n')];
    components = [(feat,val[2:-1].split(','),) for feat,val in components] if not components==[['']] else [];
    wen_string = ','.join([feat+':'+('+'.join(values)) for feat,values in components]);
    dif_string = string(D.nodes[D.index2node[child_ind]][REP]-D.nodes[D.index2node[parent_ind]][REP]);
    if dif_string == '': return new_string, wen_string, 'nothing';
    components = [comp.split(':') for comp in dif_string.split('\n')];
    components = [(feat,val[2:-1].split(','),) for feat,val in components] if not components==[['']] else [];
    dif_string = ','.join([feat+':'+('+'.join(values)) for feat,values in components]);
    return new_string, wen_string, dif_string;

def make_rep_str(child_ind,parent_ind): #TODO: include a sorting of the components
    new_string = D.nodes[D.index2node[child_ind]][STR];
    components = [comp.split(':') for comp in new_string.split('\n')];
    components = [(feat,val[2:-1].split(','),) for feat,val in components] if not components==[['']] else [];
    new_string = '; '.join([feat+':{'+(','.join(values))+'}' for feat,values in components]);
    wen_string = D.nodes[D.index2node[parent_ind]][STR];
    components = [comp.split(':') for comp in wen_string.split('\n')];
    components = [(feat,val[2:-1].split(','),) for feat,val in components] if not components==[['']] else [];
    wen_string = '; '.join([feat+':{'+(','.join(values))+'}' for feat,values in components]);
    dif_string = string(D.nodes[D.index2node[child_ind]][REP]-D.nodes[D.index2node[parent_ind]][REP]);
    if dif_string == '': return new_string, wen_string, 'nothing';
    components = [comp.split(':') for comp in dif_string.split('\n')];
    components = [(feat,val[2:-1].split(','),) for feat,val in components] if not components==[['']] else [];
    dif_string = '; '.join([feat+':{'+(','.join(values))+'}' for feat,values in components]);
    return new_string, wen_string, dif_string;

def make_handle(child_ind):
    return child_ind;

def makeforest_compressed(edges,nodeInfos,obss,carss,mentionInfos,D):
    nodes = dict();
    for child_ind,parent_ind in edges:
        child_handle        = make_handle(child_ind); #TODO: We can use the child_str and parent_str here as dictionary keys - just not for lookup
        nodes[child_handle] = dict();
    forest = dict();
    for child_ind,parent_ind in edges:
        child_handle                 = make_handle(child_ind);
        parent_handle                = make_handle(parent_ind);    # in a tree, parent_handle  could be observed more than once
        child_str,parent_str,dif_str = make_rep_str(child_ind,parent_ind);
        child_node                   = nodes[child_handle];           # in a tree,  child_handle should be observed only just once
        #print '----------------------------------------------';
        #print child_ind,'<--', parent_ind;
        #print parent_handle,'\n|\nv\n', child_handle;
        if not parent_handle in nodes:
            #print 'parent', parentparent_handle_str, 'is a root';
            nodes[parent_handle] = dict();
            forest[parent_str]   = nodes[parent_handle]; #TODO: In theory it could happen that we get another root with the same string, but it is unlikely I guess...
            temp                 = nodes[parent_handle];
            temp['observed']     = obss[parent_ind];
            temp['carries']      = carss[parent_ind];
            temp['mentions']     = mentionInfos[parent_ind];
            for component in nodeInfos[parent_ind]:
                temp[component] = nodeInfos[parent_ind][component];
            temp[child_str] = child_node;
        else:
            #print 'parent', parent_handle, 'is not a root';
            temp                        = nodes[parent_handle];
            temp[child_str]             = child_node;
            temp[child_str]['adds']     = dif_str;
            temp[child_str]['weight']   = D.weight[parent_ind,child_ind];
            temp[child_str]['observed'] = obss[child_ind];
            temp[child_str]['carries']  = carss[child_ind];
            temp[child_str]['mentions'] = mentionInfos[child_ind];
            for component in nodeInfos[child_ind]:
                temp[child_str][component] = nodeInfos[child_ind][component];
    return forest;

def makeforest_(edges,nodeInfos,obss,carss,D):
    nodes = dict();
    for child_ind,parent_ind in edges:
        child_handle        = make_handle(child_ind); #TODO: We have to use the nodeIndex to address the node as it can happen that different nodes end up with the same string
        nodes[child_handle] = {'@type':'Representation'};
    forest = dict();
    for child_ind,parent_ind in edges:
        child_handle                 = make_handle(child_ind);
        parent_handle                = make_handle(parent_ind); # in a tree, parent_handle  could be observed more than once
        child_str,parent_str,dif_str = make_rep_str(child_ind,parent_ind);
        child_node                   = nodes[child_handle];           # in a tree,  child_handle should be observed only just once
        #print '----------------------------------------------';
        #print child_ind,'<--', parent_ind;
        #print parent_handle,'\n|\nv\n', child_handle;
        if not parent_handle in nodes:
            #print 'parent', parent_handle, 'is a root';
            nodes[parent_handle] = {'@type':'Representation'};
            forest[parent_str]               = nodes[parent_handle];
            nodes[parent_handle]['@id']      = parent_str;
            nodes[parent_handle]['observed'] = obss[parent_ind];
            nodes[parent_handle]['carries']  = carss[parent_ind];
            nodes[parent_handle]['mentions'] = D.mentInfos[D.index2node[parent_ind]];
            for component in nodeInfos[parent_ind]:
                nodes[parent_handle][component] = nodeInfos[parent_ind][component];
            nodes[parent_handle]['specifications'] = {child_str:child_node};
        else:
            #print 'parent', parent_handle, 'is not a root';
            temp = nodes[parent_handle];
            if not 'specifications' in temp:
                temp['specifications'] = dict();
            temp['specifications'][child_str] = child_node;
        child_node['@id']      = child_str;
        child_node['adds']     = dif_str;
        child_node['weight']   = D.weight[parent_ind,child_ind];
        child_node['observed'] = obss[child_ind];
        child_node['carries']  = carss[child_ind];
        child_node['mentions'] = D.mentInfos[D.index2node[child_ind]];
        for component in nodeInfos[child_ind]:
            child_node[component] = nodeInfos[child_ind][component];
    return {"institution_hierarchies":forest} if edges != [] else {"institution_hierarchies":{'@id':make_rep_str(0,0)[0],'observed':obss[0],'carries':carss[0],'mentions':D.mentInfos[D.index2node[0]]}};

def makeforest(edges,nodeInfos,obss,carss,D):
    nodes = dict();
    for child_ind,parent_ind in edges:
        child_handle        = make_handle(child_ind); #TODO: We have to use the nodeIndex to address the node as it can happen that different nodes end up with the same string
        nodes[child_handle] = dict();
    forest = dict();
    for child_ind,parent_ind in edges:
        child_handle                 = make_handle(child_ind);
        parent_handle                = make_handle(parent_ind); # in a tree, parent_handle  could be observed more than once
        child_str,parent_str,dif_str = make_rep_str(child_ind,parent_ind);
        child_node                   = nodes[child_handle];           # in a tree,  child_handle should be observed only just once
        #print '----------------------------------------------';
        #print child_ind,'<--', parent_ind;
        #print parent_handle,'\n|\nv\n', child_handle;
        if not parent_handle in nodes:
            #print 'parent', parent_handle, 'is a root';
            nodes[parent_handle] = dict();
            forest[parent_str]               = nodes[parent_handle];
            nodes[parent_handle]['mentions'] = Counter([D.mentInfos[mentionIndex]['address_string'] for mentionIndex in D.NM[parent_ind,:].nonzero()[1]]);
            nodes[parent_handle]['specifications'] = {dif_str:child_node};#child_str
        else:
            #print 'parent', parent_handle, 'is not a root';
            temp = nodes[parent_handle];
            if not 'specifications' in temp:
                temp['specifications'] = dict();
            temp['specifications'][dif_str] = child_node;#child_str
        child_node['mentions'] = Counter([D.mentInfos[mentionIndex]['address_string'] for mentionIndex in D.NM[child_ind,:].nonzero()[1]]);
    return {"institution_hierarchies":forest} if edges != [] else {"institution_hierarchies":{'mentions':Counter([D.mentInfos[mentionIndex]['address_string'] for mentionIndex in D.NM[D.node2index[0],:].nonzero()[1]])}};


def tojson(D,I=None):
    print('doing transitive reduction...');
    D.edge = transitive_reduction(D.edge);
    print('start jsoning...');
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    edge_                = set_diagonal(D.edge,csr(np.zeros(D.edge.shape[0]))); #Make irreflexive to get DAG
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    weights_             = get_view(D.weight,edge_);   # Select only the weights for which there are edges in the reduced edge_
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    tree                 = max_span_tree(weights_);     # Make a tree from the DAG
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    tree                 = get_view(edge_,tree);        # Could also convert tree to boolean
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    tree_spec            = transitive_closure(set_diagonal(tree,csr(np.ones((tree.shape[0],1))))).dot(D.NM); # This is actually not so great as it introduces high compleyity theoretically, but as it is only for outputting it should be ok
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    con                  = sqlite3.connect(_cfg['root_dir']+_cfg['name_db']); cur = con.cursor();
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    edges                = [(to,fro,) for fro,to in zip(*edge_.nonzero()) if not fro==to];
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    #mentInfos            = {D.index2node[nodeIndex]: mentioninfos(nodeIndex,D,cur) for nodeIndex in xrange(D.NM.shape[0])}; #TODO: This is very inefficient because it is retrieving all mentions by ID, but it does not seem to require much memory
    #print 'MEMORY CONSUMED:', _p_.memory_info().rss/_mb_;                                          #TODO: As it does not change, it can be done once in the beginning for starters. But the nodeindexes do change
    nodeInfos,obss,carss = list(zip(*[nodeinfos(nodeIndex,tree_spec,D)   for nodeIndex in range(D.NM.shape[0])]));
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    forest               = makeforest(edges,nodeInfos,obss,carss,D);
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    context              = json.load(open(_contextfile));
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    json_ld              = forest["institution_hierarchies"];#{"@context": context["@context"], "institution_hierarchies":forest["institution_hierarchies"]};
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    #OUT                  = open(_cfg['viz_file']+'.'+str(I)+'.json','w') if I != None else open(_cfg['viz_file']+'.json','w');
    OUT                  = open(_cfg['out_dir']+'jsons/'+str(I)+'/'+_job_id+'.json','w') if I != None else open(_cfg['viz_file']+'.json','w');
    try:
        json.dump(json_ld,OUT,indent=1);
    except:
        print('Probably circular reference in forest.');
        OUT = open(_cfg['viz_file']+'.'+str(I)+'.json','w') if I != 0 else open(_cfg['viz_file']+'.json','w');
        OUT.write(str(forest));
    OUT.close(); con.close();
    print('MEMORY CONSUMED:', _p_.memory_info().rss/_mb_);
    print('done jsoning.');

def output(D,I,B,t_start,m_time,c_time,thr_iter,con_out,cur_out):
    global MEM, TIME_, SIZE;
    B              = B.split('.')[0]+', p_new:'+str(_p_new_)+', disc.:'+str(_d_)+', random:'+str(_cfg['num_rdm'])+'/'+str(_nbrdm_)+', step:'+str(_cfg['step']*[1,_cfg['selfprob_fac']][_weight_self]);
    t_iter         = datetime.datetime.utcnow().isoformat().replace('T',' ')[:-7];
    tp_b, p_b, t_b = tp_p_t(D.rids_b[:,:]);
    tp_c, p_c, t_c = tp_p_t(D.rids_c[:,:]);
    P_b, R_b, F1_b = [round(val*100,0) for val in prec_rec_f1(D.rids_b[:,:])];
    P_c, R_c, F1_c = [round(val*100,0) for val in prec_rec_f1(D.rids_c[:,:])];
    blocksum       = (D.NM.dot(D.MR.astype(int)).sum(1).A**2).sum();#(D.NM.sum(1).A**2).sum();
    num_m, num_c   = D.MC.shape;
    num_b, num_r   = D.rids_b.shape;
    general_params = (t_start,t_iter,B,_cfg['eps'],_cfg['z'],_cfg['r'],_cfg['min_pts'],thr_iter,I);
    statistics     = (num_m,num_r,MERGE,CLUS,(float(COMP.sum())/(_cfg['smooth']+(COMP>=1).sum())),blocksum,BOUND);
    performance_b  = (num_b,P_b,R_b,F1_b,tp_b,t_b,p_b);
    performance_c  = (num_c,P_c,R_c,F1_c,tp_c,t_c,p_c,MAXPR[0],MAXPR[1],MAXF1);
    cost           = (SIZE,MEM[CLU],MEM[MER],MEM[DIS],round(TIME[CLU],2),round(TIME_[SIM],2),round(TIME_[CLU],2),round(TIME[MER],2),round(TIME[DIS],2),round(m_time,2),round(c_time,2));
    additionals    = additional_output(D);
    values         = general_params + statistics + performance_b + performance_c + cost + additionals; #print values;
    cur_out.execute("INSERT INTO results VALUES("+','.join(['?' for i in range(len(values))])+")",values);
    MEM=[0,0,0]; TIME_=[0,0]; SIZE=0; #print COMP;
    con_out.commit();

def additional_output(D):
    num_oversize = 0;
    sum_oversize = 0;
    ind_oversize = [];
    for i in range(len(D.index2node)):
        if len(D.nodes[D.index2node[i]][REP]) > _oversize:
            #print 'Oversize node:'; print string(D.nodes[D.index2node[i]][REP]); print '----------------------';
            num_oversize += 1;
            sum_oversize += D.NM[i,:].sum();
            ind_oversize += [i];
    num_nodes         = D.edge.shape[0];
    num_nodes_rel     = num_nodes / float(NUM_NODES_start);
    reps_x_ment       = sum([len(D.nodes[node][REP])*D.NM[D.node2index[node],:].sum() for node in D.index2node]);
    gini_reps         = gini([len(D.nodes[node][REP])*D.NM[D.node2index[node],:].sum() for node in D.index2node]);
    gini_reps_unw     = gini([len(D.nodes[node][REP]) for node in D.index2node]);
    gini_ment         = gini(D.NM.sum(1));
    gini_reps_rel     = gini_reps / GINI_repsize_start;
    gini_reps_rel_unw = gini_reps_unw / GINI_repsize_start_unw;
    gini_ment_rel     = gini_ment / GINI_mentions_start;
    gini_cross_weight = gini_reps_rel_unw / gini_reps_rel;
    print('---------------------------------------------------');
    print('Number of nodes (start):                     ', NUM_NODES_start);
    print('Number of nodes (current):                   ', num_nodes);
    print('Number of nodes (relative):                  ', round(num_nodes_rel,2));
    print('---------------------------------------------------');
    print('Weighted Gini coefficient repsize (start):   ', round(GINI_repsize_start,2));
    print('Weighted Gini coefficient repsize (current): ', round(gini_reps,2));
    print('Weighted Gini coefficient repsize (relative):', round(gini_reps_rel,2));
    print('---------------------------------------------------'); 
    print('Unweighted gini relative to weighted gini:   ', round(gini_cross_weight,2));
    print('---------------------------------------------------');
    print('Unweight Gini coefficient repsize (start):   ', round(GINI_repsize_start_unw,2));
    print('Unweight Gini coefficient repsize (current): ', round(gini_reps_unw,2));
    print('Unweight Gini coefficient repsize (relative):', round(gini_reps_rel_unw,2));
    print('---------------------------------------------------'); 
    print('Rel.node.num x   weighted rel.repsize.gini:  ', round(num_nodes_rel*gini_reps_rel,2));
    print('---------------------------------------------------'); 
    print('Rel.node.num x unweighted rel.repsize.gini:  ', round(num_nodes_rel*gini_reps_rel_unw,2));
    print('---------------------------------------------------');
    print('... x unweighted gini relative to weighted:  ', round(num_nodes_rel*gini_reps_rel_unw*gini_cross_weight,2));
    print('---------------------------------------------------');
    return (NUM_NODES_start ,num_oversize, round(num_nodes_rel*100), round(GINI_repsize_start*100,), round(gini_reps*100), round(gini_reps_rel*100), round(gini_cross_weight*100), round(GINI_repsize_start_unw*100), round(gini_reps_unw*100), round(gini_reps_rel_unw*100), round(num_nodes_rel*gini_reps_rel*100), round(num_nodes_rel*gini_reps_rel_unw*100), round(num_nodes_rel*gini_reps_rel_unw*gini_cross_weight*100), sum_oversize, reps_x_ment);

def get_slot_statistics(nodes):
    infos = [(tuple(sorted(list(set([tup[0] for tup in nodes[node][REP]])))),nodes[node][OBS],) for node in nodes];
    types = Counter([info[0] for info in infos]);
    tokes = dict();
    for typ,freq in infos:
        if typ in tokes:
            tokes[typ] += freq;
        else:
            tokes[typ] = freq;
    stats = sorted([(tokes[typ],typ,) for typ in tokes]); print('Observed slot representations:');
    for freq,typ in stats:
        print(typ, freq, types[typ]);
#-------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS-UTILS---------------------------------------------------------------------------------------------------------------------

def gini_(x):
    x    = np.sort(np.ravel(np.asarray(x)));
    cumx = np.cumsum(x, dtype=float);
    return (len(x)+1 - 2*np.sum(cumx) / cumx[-1]) / len(x);

def gini(x, w=None):
    x = np.ravel(np.asarray(x));
    if w is not None:
        w              = np.ravel(np.asarray(w));
        sorted_indices = np.argsort(x);
        sort_x, sort_w = x[sorted_indices], w[sorted_indices];
        cumw           = np.cumsum(sort_w, dtype=float);
        cumxw          = np.cumsum(sort_x*sort_w, dtype=float);
        return (cumxw[1:]*cumw[:-1]-cumxw[:-1]*cumw[1:]).sum() / (cumxw[-1]*cumw[-1]);
    else:
        cumx = np.cumsum(np.sort(x), dtype=float);
        return (len(x)+1-2*np.sum(cumx)/cumx[-1]) / len(x);

def load_constraints(filename):
    constraints = {'requires':dict(),'forbids':dict()};
    IN = open(filename);
    for a,typ,b in [line.rstrip().split() for line in IN]:
        d = constraints[['requires','forbids'][typ=='-']];
        if a in d:
            d[a].add(b);
        else:
            d[a] = set([b]);
    IN.close();
    return constraints;

def get_view(M,M_view):
    points      = set(zip(*M.nonzero()));
    points_view = set(zip(*M_view.nonzero()));
    new_points  = points & points_view;
    if len(new_points)==0: return csr(([],([],[])),dtype=M.dtype,shape=M.shape);
    rows,cols   = list(zip(*new_points));
    data        = np.ravel(M[rows,cols]);
    M_new       = csr((data,(rows,cols)),dtype=M.dtype,shape=M.shape);
    return M_new;

def redundant_tree_(M,index2node):
    M, index2node  = copy(M), copy(index2node); print('Getting redundant tree...');
    length_,length = M.shape[0],0;
    while length_ != length: #TODO: There might be a more efficient way by somehow using np.argsort(get_node_level(D.edge))[::-1]
        i = 0;
        while i < M.shape[0]:
            M[i,i] = False;
            gens   = M[:,i].nonzero()[0][1:];
            for j in gens:
                M[j,i]     = False;
                new_column = csr([[True] if x==j else [False] for x in range(M.shape[1])]);
                M          = csr(hstack([M,new_column]));
                new_row    = M[i,:];#csr([[M[i,x] if x!=i else False for x in xrange(M.shape[0])]]); print new_row.shape
                M          = vstack([M,new_row]);
                M          = csr(M);
                index2node.append(index2node[i]);
            i += 1;
        length_ = length;
        length  = M.shape[0];
    return M,index2node;

def redundant_tree(M,index2node): #TODO: This needs to be made much faster, which should be possible but is quite tricky...
    M          = dok(M); print('Getting redundant tree...');
    i2n_new    = [];
    sort       = get_node_level(edge).argsort();
    unsort     = sort.argsort();
    M          = M[sort,:][:,sort];
    M.diagonal = False;
    index2node = list(np.array(index2node)[sort]);
    i          = 0;
    while i < M.shape[0]:
        gens   = M[:,i].nonzero()[0][1:];
        for j in gens:
            print(i,j);
            new_column = [[True] if x==j else [False] for x in range(M.shape[1])];
            M          = dok(hstack([M,new_column]));
            M[j,i]     = False;
            new_row    = M[i,:];
            M          = dok(vstack([M,new_row]));
            index2node.append(index2node[i]);
        i += 1;
    #l = len(unsort);
    #horiz_1 = hstack([M[:l,:l][unsort,:][:,unsort],M[:l,l:]]);
    #horiz_2 = hstack([M[l:,:l]                    ,M[l:,l:]]);
    #M       = vstack([horiz_1,horiz_2]);
    return csr(M), index2node;#list(index2node[:l][unsort])+list(index2node[l:]);

def max_span_tree(M): # Input needs to be DAG! #TODO: Does this maximize the probabilities of the paths?
    argmaxes = argmax(M,0);
    cols     = M.nonzero()[1];
    rows     = argmaxes[cols];
    data     = [M[rows[i],cols[i]] for i in range(len(rows))];
    M_       = csr((data,(rows,cols)),shape=M.shape,dtype=M.dtype);
    M_.eliminate_zeros();
    return M_;

def min_span_tree(M): # Input needs to be DAG!
    M.eliminate_zeros(); #TODO: Does this cause problems (it is so that no 0 edges are chosen)
    argmines = argmin(M,0);
    M_ = M[argmines,:];
    M_.eliminate_zeros();
    return M_;

def argmax(M,axis): # For old scipy versions
    if axis >= len(M.shape): print('Error: axis out of range'); return;
    nonzeros = M.nonzero();
    argmaxes = None;
    if axis == 0:
        argmaxes = [M[:,i].nonzero()[axis][np.argmax(M[:,i].data)] if len(M[:,i].data)>0 else 0 for i in range(M.shape[axis])];
    elif axis == 1:
        argmaxes = [M[i,:].nonzero()[axis][np.argmax(M[i,:].data)] if len(M[i,:].data)>0 else 0 for i in range(M.shape[axis])];
    return np.array(argmaxes);

def argmin(M,axis): # For old scipy versions
    if axis >= len(M.shape): print('Error: axis out of range'); return;
    argmines = [np.argmin(M[:,i].data) if len(M[:,i].data)>0 else 0 for i in range(M.shape[axis])];
    return argmaxes;

def analyse_sim(D,num=1000):
    pairs  = D.MR[:,:].dot(D.MR[:,:].T);
    pairs_ = list(zip(*pairs.nonzero()));
    l      = np.array([probsim(np.array([x]),D,np.array([y]))[0,0] for x,y in pairs_[:min(num,len(pairs_))]]);
    print(l);
    print(l.max(), l.min(), l.sum()/l.shape[0]);

def get_index2feat(ftype,mentionID2index,db):
    print(ftype);
    con   = sqlite3.connect(db); cur = con.cursor();
    feats = list(set([feat[0] for mentionID in mentionID2index for feat in cur.execute("SELECT feat FROM "+ftype+" WHERE mentionIDIndex=?",(mentionID,))]));
    con.close();print(len(feats),'features')
    return feats;

def get_MF(ftype,mentionID2index,feat2index,db):
    con = sqlite3.connect(db); cur = con.cursor();
    ROWS, COLS, DATA = [], [], [];print(ftype,len(feat2index))#, max(feat2index.values()), max(feat2index.keys())
    for mentionID in mentionID2index:
        for feat,freq in cur.execute("SELECT feat,freq FROM "+ftype+" WHERE mentionIDIndex=?",(mentionID,)):
            ROWS.append(mentionID2index[mentionID]); COLS.append(feat2index[feat]); DATA.append(freq);
    con.close();
    return csr((DATA,(ROWS,COLS)),shape=(len(mentionID2index),len(feat2index)),dtype=float);

def get_f(ftype,index2feat,db):
    con = sqlite3.connect(db); cur = con.cursor();
    f   = np.array([cur.execute("SELECT freq FROM "+ftype+"_sums WHERE feat=?",(feat,)).fetchall()[0][0] for feat in index2feat],dtype=float);
    con.close();
    return f;

def set_new(matrix,rs,new,COL):
    matrix.eliminate_zeros();
    rows, cols         = matrix.nonzero();
    data               = matrix.data;
    old                = np.logical_not(np.in1d( [rows,cols][COL] ,rs));
    rows_old, cols_old = rows[old], cols[old];
    data_old           = data[old];
    rows_new, cols_new = new.nonzero();
    if COL:
        cols_new = rs[cols_new];
    else:
        rows_new = rs[rows_new];
    data_new           = new[new!=0];#data_new           = np.ravel(new)[ [cols_new,rows_new][COL] ];
    cols_, rows_       = np.concatenate([cols_old,cols_new],0), np.concatenate([rows_old,rows_new],0);
    data_              = np.concatenate([data_old,data_new],0);
    return csr((data_,(rows_,cols_)),shape=matrix.shape);

def set_diagonal(matrix,new): #WARNING: new is expected to be sparse csr matrix (as opposed to what is expected in set_new)
    matrix.eliminate_zeros(); new.eliminate_zeros();
    rows, cols         = matrix.nonzero();
    data               = matrix.data;
    old                = rows!=cols;
    rows_old, cols_old = rows[old], cols[old];
    data_old           = data[old];
    rows_cols_new      = new.nonzero()[0];
    data_new           = new.data;
    cols_, rows_       = np.concatenate([cols_old,rows_cols_new],0), np.concatenate([rows_old,rows_cols_new],0);
    data_              = np.concatenate([data_old,data_new],0);
    return csr((data_,(rows_,cols_)),shape=matrix.shape);

def prec_rec_f1(rids):
    tp, p, t = [float(val) for val in tp_p_t(rids)];
    if p == 0 or t == 0: return 1.0,1.0,1.0;
    return tp/p, tp/t, 2*((tp/p)*(tp/t))/((tp/p)+(tp/t));

def tp_p_t(rids): #Assumes that you pass one block, not a block partition
    tp = rids.multiply(rids).sum();#rids.power(2).sum();
    p  = np.power(rids.sum(1),2).sum();
    t  = np.power(rids.sum(0),2).sum(1)[0,0];
    return tp, p, t;

def string(node_rep):
    if _cfg['is_names']:
        fields = set([tup[0] for tup in node_rep]);
        return ' '.join([tup[1] for tup in sorted(list(node_rep)) if not((tup[0]=='l' and 'l_' in fields) or (tup[0]=='f1' and 'f1_' in fields) or (tup[0]=='f2' and 'f2_' in fields) or (tup[0]=='f3' and 'f3_' in fields))]);
    #return '\n'.join([tup[0]+': '+tup[1] for tup in sorted(list(node_rep))]);
    node_rep_list = sorted(list(node_rep));
    type2strings = dict();
    for typ, string in node_rep_list:
        if typ in type2strings:
            type2strings[typ].append(string);
        else:
            type2strings[typ] = [string];
    vals = copy(type2strings['other']);
    for val in vals:
        for key in [key_ for key_ in type2strings.keys() if key_ != 'other']:
            if val in type2strings[key]:
                type2strings['other'].remove(val);
                break;
    if len(type2strings['other']) == 0:
        del type2strings['other'];
    return '\n'.join([_short_form[typ]+': {'+','.join(type2strings[typ])+'}' for typ in type2strings]);
    #return '\n'.join([val for typ in type2strings for val in type2strings[typ]]);
    #return '\n'.join(sorted([typ+': {'+','.join(type2strings[typ])+'}' for typ in type2strings]));

def list2string(list_rep,fields):
    string = '';
    for i in range(len(list_rep)):
        if list_rep[i] != None:
            string += fields[i]+'&"'+list_rep[i]+'";';
    return string[:-1];

def set2string(set_rep):
    list_rep = sorted(list(set_rep));
    string = '';
    for i in range(len(list_rep)):
        string += list_rep[i][0]+'&"'+list_rep[i][1]+'";';
    return string[:-1];

def generalize_(set_rep):
    d    = dict();
    for key,val in ((key,val,) for key,val in set_rep if not key=='other'):
        if key in d:
            d[key].add(val);
        else:
            d[key] = set([val]);
    gens = [set_rep];
    cur  = set([]);
    for key in [key_ for level,key_ in sorted([(_levels_[key__],key__,) for key__ in list(d.keys())])[:-1]]: #sort by level
        cur |= set(((key,val,) for val in d[key]));# | set((('other',val,) for val in d[key]));
        gens.append(copy(cur));
    return gens;

def generalize(set_rep):
    d    = dict();
    for key,val in ((key,val,) for key,val in set_rep if not key=='other'):
        if key in d:
            d[key].add(val);
        else:
            d[key] = set([val]);
    gens = [set_rep];
    cur  = set([]);
    for key in [key_ for level,key_ in sorted([(_levels_[key__],key__,) for key__ in list(d.keys())])[:-1]]: #sort by level
        cur |= set(((key,val,) for val in d[key])) | set((('other',val,) for val in d[key]));
        gens.append(copy(cur));
    return gens;

def simplify(set_rep):
    #return set([(['university','association','area','division','community','chair'][_levels_[key]],ngram,) for key,val in set_rep for ngram in ngrams(val+''.join(('_' for i in range(max([0,4-len(val)])))),4)]);
    #return set([(['university','association','area','division','community','chair'][_levels_[key]],val,) for key,val in set_rep]);
    #return set([('division' if _levels_[key]==3 else key,val,) for key,val in set_rep]);
    #['university','faculty','institute','subfield','subject'][_levels_[key]]
    return set([('other',val,) for key,val in set_rep if not val in _skipvals]) | set([(key,val,) for key,val in set_rep if key!='other' and not val in _skipvals]);
    #return set_rep;
    '''
    d = dict();
    for key,val in set_rep:
        if key in d:
            d[key].add(val);
        else:
            d[key] = set([val]);
    set_ = set([]);
    for key in d:
        if len(d[key])==1:
            set_.add(('None',d[key].pop(),));
        else:
            for val1 in d[key]:
                for val2 in d[key]:
                    if val1 != val2:
                        set_.add(('None','_'.join(sorted([val1,val2])),));
    '''
    return set_;

def ngrams(seq,n):
    return [seq[i-n:i] for i in range(n,len(seq)+1) ];

def load_node_infos_db(dbfile,key,value,typeonly):
    fields     = [field+'1' for field in set(_fields_)-_special]+list(_special) if typeonly else [field+str(num) for field in set(_fields_)-_special for num in range(1,_max_len_+1)]+list(_special);
    temp_dict  = dict();
    con        = sqlite3.connect(dbfile);
    cur        = con.cursor();    
    if key == None:
        if value == None:
            cur.execute("SELECT mentionID, id, "+', '.join(fields)+" FROM representations");
        else:
            cur.execute("SELECT mentionID, id, "+', '.join(fields)+" FROM representations WHERE "+' OR '.join([field+'=?' for field in fields]),tuple([value for field in fields]));
    elif key == 'query':
        cur.execute("SELECT mentionID, id, "+', '.join(fields)+" FROM representations WHERE mentionID IN "'('+value+')');
    elif key == 'bielefeld':
        cur.execute("SELECT mentionID, id, "+', '.join(fields)+" FROM representations WHERE id IN "'('+value+')');
    else:
        cur.execute("SELECT mentionID, id, "+', '.join(fields)+" FROM representations WHERE "+' OR '.join([[key+"=?"],[key+str(i)+"=?" for i in range(1,_max_len_+1)]][key in set(_fields_)-_special]),tuple([[value],[value for i in range(1,_max_len_+1)]][key in set(_fields_)-_special]));
    for row in cur:
        mentionID = row[0];
        rID       = str(row[1]) if row[1] != None else None;
        list_rep  = row[2:];
        set_rep   = set([(fields[i][:-1],'',) if fields[i][-1] in set([str(n) for n in range(1,_max_len_+1)]) else (fields[i],'',) for i in range(len(fields)) if list_rep[i] != None]) if typeonly else set([(fields[i][:-1],list_rep[i],) if fields[i][-1] in set([str(n) for n in range(1,_max_len_+1)]) else (fields[i],list_rep[i],) for i in range(len(fields)) if list_rep[i] != None]);
        set_rep   = simplify(set_rep);
        generalizations = generalize(set_rep);
        set_reps        = generalizations;#[simplify(set_rep_) for set_rep_ in generalizations];
        gen_num         = 0;
        for set_rep_ in set_reps:
            freq       = 1 if set_rep_ == set_rep else 0;
            mentionID_ = mentionID + '_' + str(gen_num) if freq == 0 else mentionID;
            rID_       = rID #if set_rep_==set_rep else '00000000000_None';
            if len(set_rep_)==0: continue; #TODO: How does it happen that there is an empty set representation?
            key_rep   = set2string(set_rep_);
            if not key_rep in temp_dict:
                temp_dict[key_rep] = [set_rep_,{rID:Counter({mentionID_:freq})}];
            else:
                if not rID in temp_dict[key_rep][1]:
                    temp_dict[key_rep][1][rID]  = Counter({mentionID_:freq});
                else:
                    temp_dict[key_rep][1][rID] += Counter({mentionID_:freq});
            gen_num += 1 if freq == 0 else 0;
    node_infos = [temp_dict[key_rep] for key_rep in temp_dict];
    con.close();
    return node_infos;

def compress(type_list):
    return hash(tuple(sorted([_field2index[el] for el in type_list])));

def load_lattice(latfile):
    lat_con = sqlite3.connect(latfile);
    lat_cur = lat_con.cursor();
    return lat_cur;

def in_lattice(type_list,lat_cur):
    element_ = compress(type_list);
    for row in lat_cur.execute("SELECT element FROM allowed WHERE element=?",(element_,)):
        return True;
    else:
        return False;

def make_node(node_info,aggregate):
    node = [sum(node_info[1].values()),0.0,set([]),set([]),node_info[0],string(node_info[0]),None,node_info[1],set([]),get_type(node_info[0])] if aggregate else [sum([sum(node_info[1][rid].values()) for rid in node_info[1]]),0.0,set([]),set([]),node_info[0],string(node_info[0]),None,node_info[1],set([]),get_type(node_info[0])];
    return node;

def get_nodes_by_level(nodes):
    nodes_by_level = dict();
    for spe_str in nodes:
        level = len(nodes[spe_str][REP]);
        if level in nodes_by_level:
            nodes_by_level[level].add(spe_str);
        else:
            nodes_by_level[level] = set([spe_str]);
    return nodes_by_level;

def get_nodes_by_level_matrix(M):
    level = get_node_level(M);
    nodes_by_level = dict();
    for node_index in range(len(level)):
        if level[node_index] in nodes_by_level:
            nodes_by_level[level[node_index]].append(node_index);
        else:
            nodes_by_level[level[node_index]] = [node_index];
    return nodes_by_level;

def get_nodes_by_lat_level(nodes):
    nodes_by_level = dict();
    for spe_str in nodes:
        level = (len(set([tup[0] for tup in nodes[spe_str][REP]])),len(nodes[spe_str][REP]),);
        if level in nodes_by_level:
            nodes_by_level[level].add(spe_str);
        else:
            nodes_by_level[level] = set([spe_str]);
    return nodes_by_level;

def get_type(node_rep):
    return tuple(sorted([el[0] for el in node_rep]));

def sanity_check(D):
    for i in range(len(D.index2node)):
        car_cnt = D.car[i,0];
        sum_cnt = D.obs.toarray()[np.ravel(D.spec[i].toarray())].sum();
        if abs(car_cnt-sum_cnt) > 0.000000001:
            print('###WARNING!', D.index2node[i], 'sum:', sum_cnt, 'vs.', car_cnt);
            print('specifications:', [D.index2node[j] for j in D.spec[i].nonzero()[1]]);
            print('--------------------------------------------------------');

def clean_nodes(nodes):
    nodes_ = dict();
    nodes_by_level = get_nodes_by_level(nodes);
    for level in sorted(list(nodes_by_level.keys()),reverse=False): #TODO: Or reverse=True?
        for node_gen in nodes_by_level[level]:
            if nodes[node_gen][OBS]==0 and len(nodes[node_gen][GEN])==0:
                for node_spe in nodes[node_gen][SPE]:
                    nodes[node_spe][GEN].remove(node_gen); # remove the node from the GENs of its SPEs
            else:
                nodes_[node_gen] = nodes[node_gen];     # do not copy the node
    print('Cleaning nodes... #nodes:', len(nodes), len(nodes_));
    return nodes_;

def clean_all_nodes(nodes):
    nodes_ = dict();
    nodes_by_level = get_nodes_by_level(nodes);
    for level in sorted(nodes_by_level,reverse=True):
        for node_mid in nodes_by_level[level]: # If unobserved and not two or more specifications where at least one of them has different slots
            if nodes[node_mid][OBS] == 0 and (_clean_without_exception or (len(nodes[node_mid][SPE])<2 or not sum([not same_slots(nodes[node_mid][REP],nodes[node_spe][REP]) for node_spe in nodes[node_mid][SPE]]))):
                #print 'REMOVING...'; print node_mid; print '-------------------';
                for node_spe in nodes[node_mid][SPE]:
                    nodes[node_spe][GEN] -= set([node_mid]);      # remove the node        from the GENs of its SPEs (if it is still there)
                    nodes[node_spe][GEN] |= nodes[node_mid][GEN]; # add    the node's GENs to   the GENs of its SPEs
                for node_gen in nodes[node_mid][GEN]:
                    nodes[node_gen][SPE] -= set([node_mid]);      # remove the node        from the SPEs of its GENs (if it is still there)
                    nodes[node_gen][SPE] |= nodes[node_mid][SPE]; # add    the node's SPEs to   the SPEs of its GENs
            else:
                nodes_[node_mid] = nodes[node_mid];     # do not copy the node
    for node in nodes_:
        nodes_[node][SP_] = set([sp_ for sp_ in nodes_[node][SP_] if sp_ in nodes_]);
    print('Cleaning all nodes... #nodes:', len(nodes), len(nodes_));
    return nodes_;

def complete_reps(D,up=False,all_slots=False):
    nodes_by_level = get_nodes_by_level_matrix(D.edge);
    for level in sorted(nodes_by_level,reverse=(not up)):
        for node_index_fro in nodes_by_level[level]:
            node_fro   = D.index2node[node_index_fro];
            to_indices = D.edge[node_index_fro,:].nonzero()[1] if up else D.edge[:,node_index_fro].nonzero()[0];
            for node_to in [D.index2node[node_index_to] for node_index_to in to_indices]:
                slots_to = set([tup[0] for tup in D.nodes[node_to][REP]]);
                for slot,value in D.nodes[node_fro][REP]:
                    if all_slots or slot in slots_to:
                        D.nodes[node_to][REP].add((slot,value,));
                D.nodes[node_to][STR] = string(D.nodes[node_to][REP]);

def complete_slots(gen_rep,spe_rep):
    gen_slots = dict();
    for slot,value in gen_rep:
        if slot in gen_slots:
            gen_slots[slot].add(value);
        else:
            gen_slots[slot] = set([value]);
    spe_slots = dict();
    for slot,value in spe_rep:
        if slot in spe_slots:
            spe_slots[slot].add(value);
        else:
            spe_slots[slot] = set([value]);
    for slot in gen_slots:
        if slot in spe_slots:
            gen_slots[slot] |= spe_slots[slot];
    for slot in spe_slots:
        if slot in gen_slots:
            spe_slots[slot] |= gen_slots[slot];
    new_gen = set([(slot,value,) for slot in gen_slots for value in gen_slots[slot]]);
    new_spe = set([(slot,value,) for slot in spe_slots for value in spe_slots[slot]]);
    print('########################'); print(string(gen_rep)); print('________________________'); print(string(spe_rep));
    print('########################'); print(string(new_gen)); print('________________________'); print(string(new_spe));
    return new_gen, new_spe;

def transitive_reduction(M):
    edges     = set_diagonal(M,csr(np.zeros(M.shape[0],dtype=bool)[:,None]));
    reduction = edges.copy();
    num, i    = 1,2;
    while num > 0 and i <= len(_fields_)*_max_len_+1: #TODO: the smaller should not be required but it seems that sometimes there are cycles in the graph
        new        = edges**i;
        num        = len(new.nonzero()[0]);
        reduction  = reduction > new;
        #print '...',i,':',num;
        i += 1;
        reduction.eliminate_zeros();
        if reduction.diagonal().sum() > 0:
            print('WARNING: Cycles in input matrix!');
    return set_diagonal(reduction,csr(M.diagonal()[:,None])).astype(bool);

def get_node_level(M):
    edges    = set_diagonal(M.T,csr(np.zeros(M.shape[0],dtype=bool)[:,None]));
    previous = edges**0;
    level    = np.zeros(edges.shape[0],dtype=int);
    nodes    = np.arange(edges.shape[0],dtype=int);
    num, i   = 1,1;
    while len(nodes) > 0 and i <= len(_fields_)*_max_len_+1: #TODO: the smaller should not be required but it seems that sometimes there are cycles in the graph
        new          = edges**i;
        nodes        = (new>previous).sum(1).nonzero()[0];#For each node: Can I reach any node with i steps that I could not reach with i-1 steps?
        level[nodes] = i;
        previous     = new;
        i           += 1;
        if previous.diagonal().sum() > 0:
            print('WARNING: Cycles in input matrix!');
    return level;

def transitive_closure(M):
    edges     = set_diagonal(M,csr(np.zeros(M.shape[0],dtype=bool)[:,None]));
    closure   = edges.copy();
    num, i    = 1,2;
    while num > 0 and i <= len(_fields_)*_max_len_+1: #TODO: the smaller should not be required but it seems that sometimes there are cycles in the graph
        print('...',i,':',num);
        new        = edges**i;
        num        = len(new.nonzero()[0]);
        closure    = closure + new;
        i         += 1;
        closure.eliminate_zeros();
        if closure.diagonal().sum() > 0:
            print('WARNING: Cycles in input matrix!');
    return set_diagonal(closure,csr(M.diagonal()[:,None])).astype(bool);

def get_ok_slot(D):
    rows, cols = list(zip(*[(i,j) for i,j in zip(*D.edge.nonzero()) if can_be_merged(i,j,D) and should_be_merged(i,j,D)]));
    return csr((np.ones(len(rows),dtype=bool),(rows,cols)),shape=D.edge.shape, dtype=bool);
#-------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS-COMPARISON----------------------------------------------------------------------------------------------------------------

def licenced_(node_rep):
    typ = get_type(node_rep);
    if in_lattice(typ,lat_cur):
        return True;
    return False;

def licenced(node_rep):
    if not _licensing:
        return True if len(node_rep) >= 2 else False;
    maxnumcomp = max([0]+list(Counter(get_type(node_rep)).values()));
    components = set(get_type(node_rep));
    if len(components) == 0 or maxnumcomp > _max_len_: return False;
    for component in components:
        if component in _constraints['requires']:
            requirement_fulfilled = False;
            for requirement in _constraints['requires'][component]:
                if requirement in components:
                    requirement_fulfilled = True;
                    break;
            if not requirement_fulfilled: return False;
        if component in _constraints['forbids']:
            for banned in _constraints['forbids'][component]:
                if banned in components:
                    return False;
    return True;

def match(reps):
    return licenced(set().union(*reps));

def same_slots(rep1,rep2):
    slots1, slots2 = set([tup[0] for tup in rep1]), set([tup[0] for tup in rep2]);
    return len(slots1) == len(slots2) and len(slots1|slots2) == len(slots2);

def should_be_merged(i,j,D):
    #gen_rep, spe_rep = D.nodes[D.index2node[i]][REP], D.nodes[D.index2node[j]][REP];
    #ref_prob    = D.weight[i,i];#*D.weight[i,j];
    #if ref_prob < _min_self_prob and D.edge[i,:].sum()>2:
    #    #print gen_rep; print ref_prob; print spe_rep; print D.edge[i,:].sum(); print '----------------------------------';
    #    return False;
    return True;

def can_be_merged(i,j,D):
    if not _slot_merge:
        return True;
    gen_rep, spe_rep = D.nodes[D.index2node[i]][REP], D.nodes[D.index2node[j]][REP];
    gen_vals         = set([val for key,val in gen_rep if key=='other']);
    spe_vals         = set([val for key,val in spe_rep if key=='other']);
    spe_vals_only    = spe_vals - gen_vals;
    gen_vals_only    = gen_vals - spe_vals;
    gen_keys         = set([key for key,val in gen_rep if key!='other']);
    spe_keys         = set([key for key,val in spe_rep if key!='other']);
    spe_keys_only    = spe_keys - gen_keys;
    gen_keys_only    = gen_keys - spe_keys;
    max_spe_key_only = max([_levels_[key] for key in spe_keys_only]) if len(spe_keys_only) > 0 else 0;
    max_gen_key      = max([_levels_[key] for key in gen_keys     ]) if len(spe_keys_only) > 0 else 4;
    #print(gen_rep); print(spe_rep); print(val_diff); print('--------------------------');
    return len(spe_vals_only)==0# or (len(spe_keys_only)+len(gen_keys_only)==0 and len(gen_keys) >= 3);# or (max_spe_key_only < max_gen_key and len(gen_keys) >= 3);
    #gen_levels       = [_levels_[key] for key,val in gen_rep if key!='other'];
    #gen_max_lvl      = max(gen_levels) if len(gen_levels)>0 else _levels_['other'];
    #level_of         = dict();
    #for val in val_diff:
    #    level_of[val] = min([_levels_[key] for key,val_ in spe_rep if val_==val]); # The most general label for the additional value
    #return len(level_of)==0 or max([level_of[val] for val in level_of]) < gen_max_lvl; # The most specific of the most general labels must be more general than the most specific label in gen_rep except other
    #diff_keys        = set([key for key,val in spe_rep if val in val_diff]); # Empty if no value difference
    #diff_max_lvl     = max([_levels_[key] for key in diff_keys]) if len(diff_keys) > 0 else 0;
    # Does not exist a most specific diff_key that is more or equally specific than the most specific key in gen_rep
    return diff_max_lvl <= gen_max_lvl; #TODO: The equal can cause problems
    # The most specific diff_key must be more general than the most specific key in gen_rep
    return less_vals; #or (same_slots(gen_rep,spe_rep) and less_vals);
    #--------------- BELOW IS SKIPPED ---------------
    if same_slots(gen_rep,spe_rep):
        return generalizes(gen_rep,spe_rep) and set([val for key,val in gen_rep if key=='other'])==set([val for key,val in spe_rep if key=='other']); #True
    if not _allow_complete_merges:
        return False;
    slots_gen, slots_spe = set([tup[0] for tup in gen_rep]), set([tup[0] for tup in spe_rep]);
    slots_spe_only       = slots_spe - slots_gen;
    if len(slots_spe_only) == 0:
        return False;
    max_level_gen        = max([_levels_[slot] for slot in slots_gen]);
    max_level_spe_only   = max([_levels_[slot] for slot in slots_spe_only]);
    # Only allow merging if the the child is a generalization except for some slots that only exists in the child
    # and that are more general than any slot in the parent
    spe_red = set([(key,val,) for key,val in spe_rep if not key in slots_spe_only]);
    return max_level_gen > max_level_spe_only and generalizes(spe_red,gen_rep);
    #if len(slots_spe_only)==0: # This is really the part that creates oversize nodes
        #print 'Specification has less slots than generalization:'; print gen_rep; print spe_rep;
    #    return True;
    #max_level_gen        = max([_levels_[slot] for slot in slots_gen]);
    #max_level_spe_only   = max([_levels_[slot] for slot in slots_spe_only]);
    #return max_level_gen > max_level_spe_only;

def generalizes(rep1,rep2): #generalizes itself, too
    return len(rep1-rep2)==0;

def frob_for(ftype,xs,D,xs_=np.array([]),MAX=False):
    global MEM;
    xs_      = xs_ if xs_.any() else xs;
    one_by_f = csr(D.one_by_f[ftype]);
    p_x_f    = D.MF[ftype][xs_,:].multiply(one_by_f*_cfg['hack']).tocsr();     mem=_p_.memory_info().rss/_mb_; MEM[CLU]=max(MEM[CLU],mem); #print mem,'MB used after p_x_f';#TODO:change back!
    N        = D.MF[ftype].shape[0];                                    mem=_p_.memory_info().rss/_mb_; MEM[CLU]=max(MEM[CLU],mem); #print mem,'MB used after N';
    num      = D.MF[ftype][xs,:].dot(p_x_f.T).toarray()+_cfg['smooth']/N;     mem=_p_.memory_info().rss/_mb_; MEM[CLU]=max(MEM[CLU],mem); #print mem,'MB used after num';
    f_x_x    = num if not MAX else np.maximum(num,num.T);
    return f_x_x;

def prob_for(ftype,xs,D,xs_=np.array([]),MAX=False):
    xs_ = xs_ if xs_.any() else xs;
    f_x_x = frob_for(ftype,xs,D,xs_,MAX);
    f_x   = D.freq_x[ftype][:,xs_]+_cfg['smooth'];
    p_x_x = np.array(f_x_x / f_x);
    return p_x_x;

def probsim(xs,D,xs_=np.array([]),ftypes=None,MAX=False):
    global TIME_;
    xs_    = xs_    if xs_.any() else xs;
    ftypes = ftypes if ftypes != None else list(D.MF.keys());
    #print 'similarity';
    t_sim      = time.time();
    similarity = np.zeros((len(xs),len(xs_)),dtype=float);
    for ftype in ftypes:
        #print ftype;
        p_x_x = prob_for(ftype,xs,D,xs_,MAX);
        similarity += p_x_x*(1./len(ftypes));
        del p_x_x;
    TIME_[SIM] += time.time()-t_sim;
    #print 'end similarity';
    return similarity;

def cosine(ftype,xs,D,xs_=np.array([])): #TODO:Smoothing required?
    xs_    = xs_ if xs_.any() else xs;
    num    = D.MF[ftype][xs,:].dot(D.MF[ftype][xs_,:].T).toarray();
    norm   = np.sqrt(D.MF[ftype][xs,:].multiply(D.MF[ftype][xs,:]).sum(1));
    norm_  = np.sqrt(D.MF[ftype][xs_,:].multiply(D.MF[ftype][xs_,:]).sum(1));
    denom  = norm*norm_.T;
    result = np.nan_to_num(num/denom);
    return result.A;    

def cosim(xs,D,xs_=np.array([]),ftypes=None,MAX=False):
    global TIME_;
    xs_        = xs_    if xs_.any() else xs;
    ftypes     = ftypes if ftypes != None else list(D.MF.keys());
    t_sim      = time.time();
    similarity = np.zeros((len(xs),len(xs_)),dtype=float);
    for ftype in ftypes:
        result      = cosine(ftype,xs,D,xs_);
        similarity += result*(1./len(ftypes));
        del result;
    TIME_[SIM] += time.time()-t_sim;
    return similarity;

def euclidist(xs,D,xs_=np.array([]),ftypes=None):
    xs_    = xs_    if xs_.any() else xs;
    ftypes = ftypes if ftypes != None else list(D.MF.keys());
    euclid = dict(); #print 'similarity';
    for ftype in ftypes:
        euclid[ftype] = pdist(D.MF[ftype][xs,:],metric='euclidean');#print ftype;
    similarity = np.sum([euclid[ftype]*1./len(euclid) for ftype in euclid],0); #print 'end similarity';
    return similarity;

def sim(xs,D,xs_=np.array([])):
    if _similarity_ == 'probsim':   return probsim(xs,D,xs_);
    if _similarity_ == 'euclidist': return euclidist(xs,D,xs_);
    if _similarity_ == 'cosim':     return cosim(xs,D,xs_);

def reach(similarity):
    N         = similarity.shape[0];
    threshold = get_threshold(N);
    reachable = similarity <= threshold if _similarity_ in ['euclidist'] else similarity >= threshold;
    np.fill_diagonal(reachable,True);
    return reachable;
#-------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS-BUILDING------------------------------------------------------------------------------------------------------------------

def find_edges_(reps):
    reps,s2i = list(zip(*[(tup[1],tup[2],) for tup in sorted([(len(reps[i]),reps[i],i,) for i in range(len(reps))],reverse=True)]));
    edges    = [set([])];
    spe_s    = [set([]) for i in range(len(reps))];
    start    = {len(reps[1]):0};
    for i in range(1,len(reps)):
        if i % 100 == 0: print(i);
        edges.append(set([]));
        for size in start:
            start[size] += 1;
        if len(reps[i]) < len(reps[i-1]):
            start[len(reps[i])] = 0;
        compare = OS(range(i-start[len(reps[i])]));
        while len(compare) > 0:
            j = compare.pop();
            if generalizes(reps[i],reps[j]):
                edges[-1].add(j);
                spe_s[i].add(j);
                spe_s[i] |= spe_s[j];
                compare  -= spe_s[j];
    return edges,spe_s,s2i;

def find_edges_(reps):
    reps,s2i = list(zip(*[(tup[1],tup[2],) for tup in sorted([(len(reps[i]),reps[i],i,) for i in range(len(reps))],reverse=True)]));
    edges    = [set([])];
    spe_s    = [set([]) for i in range(len(reps))];
    start    = {len(reps[1]):0};
    for i in range(1,len(reps)):
        if i % 100 == 0: print(i);
        edges.append(set([]));
        for size in start:
            start[size] += 1;
        if len(reps[i]) < len(reps[i-1]):
            start[len(reps[i])] = 0;
        compare = OD(list(zip(list(range(i-start[len(reps[i])])),[None for x in range(i-start[len(reps[i])])])));
        while len(compare) > 0:
            j = list(compare.keys())[-1]; compare.pop(j);
            if generalizes(reps[i],reps[j]):
                edges[-1].add(j);
                spe_s[i].add(j);
                spe_s[i] |= spe_s[j];
                for k in edges[j]:
                    compare.pop(k,None);
    return edges,spe_s,s2i;

def find_edges_spe(reps): #s2i gives the position of the indices produced here in edges and spe_ in the original input list of representations
    if len(reps)==1: return dict(), dict(), dict(), dict();
    reps,s2i = list(zip(*[(tup[1],tup[2],) for tup in sorted([(len(reps[i]),reps[i],i,) for i in range(len(reps))],reverse=True)]));
    edges    = [set([])];
    spe_s    = [set([]) for i in range(len(reps))];
    #print '#########################################\n',reps,'#########################################';
    start    = {len(reps[1]):0};
    for i in range(1,len(reps)):
        if i % 100 == 0: print(i);
        edges.append(set([]));
        for size in start:
            start[size] += 1;
        if len(reps[i]) < len(reps[i-1]):
            start[len(reps[i])] = 0;
        compare = OS(range(i-start[len(reps[i])]));
        while len(compare) > 0:
            j = compare.pop();
            if generalizes(reps[i],reps[j]):
                edges[-1].add(j);
                spe_s[i].add(j);
                spe_s[i] |= spe_s[j];
                compare  -= spe_s[j];
    spes, gens = dict(), dict();
    for i in range(len(edges)):
        spes[s2i[i]] = set([s2i[j] for j in edges[i]])
        for j in edges[i]:
            if s2i[j] in gens:
                gens[s2i[j]].add(s2i[i]);
            else:
                gens[s2i[j]] = set([s2i[i]]);
    sp_s, ge_s = dict(), dict();
    for i in range(len(spe_s)):
        sp_s[s2i[i]] = set([s2i[j] for j in spe_s[i]])
        for j in spe_s[i]:
            if s2i[j] in ge_s:
                ge_s[s2i[j]].add(s2i[i]);
            else:
                ge_s[s2i[j]] = set([s2i[i]]);
    return spes, gens, sp_s, ge_s;

def find_edges_gen(reps): #s2i gives the position of the indices produced here in edges and spe_ in the original input list of representations
    if len(reps)==1: return dict(), dict(), dict(), dict();
    reps,s2i = list(zip(*[(tup[1],tup[2],) for tup in sorted([(len(reps[i]),reps[i],i,) for i in range(len(reps))],reverse=False)]));
    edges    = [set([])];
    gen_s    = [set([]) for i in range(len(reps))];
    start    = {len(reps[1]):0};
    for i in range(1,len(reps)):
        if i % 100 == 0: print(i);
        edges.append(set([]));
        for size in start:
            start[size] += 1;
        if len(reps[i]) > len(reps[i-1]):
            start[len(reps[i])] = 0;
        compare = OS(range(i-start[len(reps[i])]));
        while len(compare) > 0:
            j = compare.pop();
            if generalizes(reps[j],reps[i]):
                edges[-1].add(j);
                gen_s[i].add(j);
                gen_s[i] |= gen_s[j];
                compare  -= gen_s[j];
    spes, gens = dict(), dict();
    for i in range(len(edges)):
        gens[s2i[i]] = set([s2i[j] for j in edges[i]])
        for j in edges[i]:
            if s2i[j] in spes:
                spes[s2i[j]].add(s2i[i]);
            else:
                spes[s2i[j]] = set([s2i[i]]);
    sp_s, ge_s = dict(), dict();
    for i in range(len(gen_s)):
        ge_s[s2i[i]] = set([s2i[j] for j in gen_s[i]])
        for j in gen_s[i]:
            if s2i[j] in sp_s:
                sp_s[s2i[j]].add(s2i[i]);
            else:
                sp_s[s2i[j]] = set([s2i[i]]);
    return spes, gens, sp_s, ge_s;

def find_min_els(repIDs,ID2rep):
    min_els = set(repIDs);
    for x in repIDs:#[tup[1] for tup in sorted([(len(ID2rep[repID]),repID,) for repID in repIDs])]:
        check = False;
        for min_el in min_els:
            if generalizes(ID2rep[min_el],ID2rep[x]) and min_el!=x:
                check = True;
                break;
        if check:
            min_els.remove(x);
    return min_els;

def insert(spe_rep,spe_str,count,seen,nodes):
    if spe_str in _minels: return;
    for tup in spe_rep:
        gen_rep = spe_rep - set([tup]);
        if not licenced(gen_rep): continue;#_licensing and #print '--------------------------------------'; print string(gen_rep), 'is not licenced.'; continue;
        gen_str = string(gen_rep);
        if gen_str in nodes:
            nodes[spe_str][GEN].add(gen_str);
            nodes[gen_str][SPE].add(spe_str);
            nodes[gen_str][SP_] |= seen|set([spe_str]);
            if nodes[gen_str][MOD] != iteration:
                nodes[gen_str][MOD]  = iteration;
                nodes[gen_str][CAR] += count;
                insert(gen_rep,gen_str,count,seen|set([spe_str]),nodes);
        else:
            nodes[gen_str] = [0,count,set([spe_str]),set([]),gen_rep,gen_str,iteration,Counter(),seen|set([spe_str]),get_type(gen_rep),set([])];
            nodes[spe_str][GEN].add(gen_str);
            insert(gen_rep,gen_str,count,seen|set([spe_str]),nodes);

def add_node(spe_rep,rids,aggregate,nodes):
    global iteration;
    iteration += 1;
    spe_str    = string(spe_rep);
    count      = sum(rids.values()) if aggregate else sum([len(lst) for lst in list(rids.values())]);
    if spe_str in nodes:
        nodes[spe_str][OBS] += count;
        nodes[spe_str][CAR] += count;
        nodes[spe_str][RID] += rids;
        nodes[spe_str][MOD]  = iteration;
        nodes[spe_str][PTS]  = set([spe_str]);
    else:
        nodes[spe_str] = [count,count,set([]),set([]),spe_rep,spe_str,iteration,rids,set([]),get_type(spe_rep),set([spe_str])];
    insert(spe_rep,spe_str,count,set([]),nodes);
#-------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS-MODIFICATION--------------------------------------------------------------------------------------------------------------

def combine(matrix,group,r,keep,reach=False,incidence=False):
    t_ = time.time(); #print 'Start shape:', matrix.shape;
    # Representant gets combination of values of group | Sets the rth row to be the sum of the group rows
    matrix = set_new(matrix,np.array([r]),matrix[group,:].toarray().sum(0,matrix.dtype)[None,:],False);
    #print 'A', time.time()-t_, matrix.shape, len(matrix.nonzero()[0]), reach; t = time.time();
    # If the matrix is quadratic (D.edge, D.spec), then whatever goes to group, also goes to r | Sets the rth column to be the sum of the group columns
    if incidence:
        matrix = set_new(matrix,np.array([r]),matrix[:,group].toarray().sum(1,matrix.dtype)[:,None],True);
    #print 'B', time.time()-t, matrix.shape, len(matrix.nonzero()[0]), reach; t = time.time();
    # If this applies (D.spec), whatever reaches r, now also reaches what r reaches | Adds rth row to all rows with 1 in rth column
    if reach:
        reaches_r = D.spec[:,r].nonzero()[0];
        if len(reaches_r) != 0:
            matrix = set_new(matrix,reaches_r,matrix[reaches_r,:].toarray()+matrix[r,:].toarray(),False);
    #print 'C', time.time()-t, matrix.shape, len(matrix.nonzero()[0]), reach; t = time.time();
    # Everything in group except representant gets their values removed | Makes the matrix smaller
    if incidence:
        matrix = matrix[keep,:][:,keep];
    else:
        matrix = matrix[keep,:];
    #print 'D', time.time()-t, matrix.shape, len(matrix.nonzero()[0]), reach; t = time.time();
    #print 'E', time.time()-t; t = time.time();
    #print 'Combined. Took', time.time()-t_, 'seconds for', matrix.shape;
    return matrix;

def components(edges,core,oldlabels): #TODO: Check if edges should be sparse
    global MEM, TIME_;
    t_clu     = time.time();
    label     = 0; #print 'components';
    edges     = csr(edges,dtype=bool,shape=edges.shape);
    labelling = np.array(range(len(core),len(core)*2),dtype=int);
    remaining = np.copy(core);
    reachable = np.zeros(len(core),dtype=bool);
    visited   = np.zeros(len(core),dtype=bool);
    while remaining.any():
        #print 'DBSCAN iteration...';
        if not reachable.any():
            start            = remaining.argmax();#print 'Nothing reachable, start with remaining no.\n', start;
            label            = oldlabels[start];#print 'This one used to have label:\n', label;
            reachable[start] = True;
        else:
            visited += reachable;#print 'So far we have visited:',visited.nonzero()[0]; print np.in1d(visited.nonzero()[0],remaining.nonzero()[0]).nonzero(); print edges[start,start];
        #print 'Reachable before taking closure:', reachable.nonzero()[0]; print 'Remaining node in visited?', np.in1d(visited.nonzero()[0],remaining.nonzero()[0]).nonzero()[0]; print 'Node reaches itself?', edges[start,start];
        #print csr(reachable).shape, edges.shape;
        reachable            = np.ravel(csr(reachable).dot(edges).toarray()) > visited; mem=_p_.memory_info().rss/_mb_; MEM[CLU]=max(MEM[CLU],mem); #print mem,'MB used'; print 'Add all unvisited nodes reachable from what was last reached:\n', reachable.nonzero()[0]; print 'Start remaining?',remaining[start],'reachable?', reachable[start];
        labels               = np.unique(oldlabels[reachable]);#print 'This set of reachable nodes used to have one of the labels\n', labels;
        reachable            = (reachable + np.in1d(oldlabels,labels)) > visited; mem=_p_.memory_info().rss/_mb_; MEM[CLU]=max(MEM[CLU],mem); #print mem,'MB used'; print 'Fast Forward: Add all nodes that used to have one of these labels:\n', reachable.nonzero()[0];
        labelling[reachable] = label;#print 'Make new labelling:\n', labelling; print 'Start remaining?',remaining[start],'reachable?', reachable[start];
        remaining            = remaining > reachable; #print 'Remaining is what was remaining before and has not been reached:\n', remaining.nonzero()[0];
    visited             += reachable;
    reachable            = np.ravel(csr(reachable).dot(edges).toarray()) > visited; mem=_p_.memory_info().rss/_mb_; MEM[CLU]=max(MEM[CLU],mem); #print mem,'MB used';#TODO: Should I remove these visited?
    labelling[reachable] = label;
    labels, labelling    = np.unique(labelling,return_inverse=True); #print 'end components';
    TIME_[CLU] += time.time() - t_clu;
    return len(labels), labelling;

def DBSCAN(D,local):
    # Compute the similarities and reachability for the local context
    sim_loc               = sim(local,D);   #TODO: See if it makes sense to keep global sim table and reuse the old sim, requires mem
    reach_loc             = reach(sim_loc);
    # Compute the core property for the local context
    core_loc              = np.ravel(reach_loc.sum(1) >= _cfg['min_pts']);#print csr(core_loc[None,:]).shape, reach_loc.shape;
    # Compute the arrows for the local context
    arrow_loc             = core_loc[:,None] * reach_loc;
    # Cluster the mentions in the local context
    n_loc, labelling_loc  = components(arrow_loc,core_loc,D.labelling[local]);
    # Integrate the new local labelling into the global context
    labelling_new         = labelling_loc+D.n;
    #labelling             = copy(D.labelling);
    D.labelling[local]    = labelling_new;
    # Update the global labelling and global n
    D.labels, D.labelling = np.unique(D.labelling,return_inverse=True);
    D.n                   = len(D.labels);
    return D;

def logistic(t,G,k,f0):
    return G/( 1 + (np.e**(-k*G*t) * ((G/f0)-1)) );

def root(x,s,n,k):
    return (s*(x**(1.0/n)))-k;

def logist_2(x,h,m,s):
    return logistic(x,h,h,s) + logistic(x,h,h/m,(s/(m*2000.)));

def get_threshold(N):
    if _cfg['tuning']:         return 0.0;
    if _dbscan_:               return _cfg['eps'];
    if _cfg['thr_f']=='root':  return root(    N,_cfg['z'],_cfg['r'],_cfg['eps']);
    if _cfg['thr_f']=='logi':  return logistic(N,_cfg['z'],_cfg['r'],_cfg['eps']);
    if _cfg['thr_f']=='2logi': return logist_2(N,_cfg['z'],_cfg['r'],_cfg['eps']);

def visualize(rids_c): #TODO: Need to find a way to show Recall deficits
    rids_c = rids_c.toarray();
    select = rids_c[:,:][rids_c[:,:].sum(1)>0,:];
    string = str([[el for el in line if el != 0] for line in select]);
    return string;

def AGGLO(D,local):   
    global MAXF1, MAXPR;
    MAXF1, MAXPR = 0., [0.,0.];
    #-Compute the iteration-independent components-----------------------------------------------------------------
    N          = len(local);
    C          = N;
    ftypes     = list(D.MF.keys());
    threshold  = get_threshold(N);
    old_string = '';
    #--------------------------------------------------------------------------------------------------------------
    #-Initialize the iteration-dependent components----------------------------------------------------------------
    MC     = np.identity(N,bool);                                                           # dense
    f_C_C  = np.array(np.concatenate([frob_for(ftype,local,D)[:,:,None] for ftype in ftypes],axis=2)); #(C,C',ftype)
    #f_C    = np.concatenate([np.array((D.MF[ftype][local,:].T.sum(0)+_cfg['smooth'])[:,:,None]) for ftype in ftypes],axis=2); #(1,C',ftype)
    f_C    = np.concatenate([np.array((D.freq_x[ftype][:,local]+_cfg['smooth'])[:,:,None]) for ftype in ftypes],axis=2)
    p_C_C  = (f_C_C / f_C);
    scores = p_C_C.sum(2) / len(ftypes); #print scores.shape;
    np.fill_diagonal(scores,0);
    #--------------------------------------------------------------------------------------------------------------
    #-Tuning-relevant measures-------------------------------------------------------------------------------------
    min_sim = 1.0; max_f1 = 0.0; stamp = datetime.datetime.utcnow().isoformat().replace('T',' ');
    #--------------------------------------------------------------------------------------------------------------
    while C > 1:
        #print p_C_C.sum(0);
        rids_c = csr(MC).T.dot(D.MR_[local,:]);
        #string = visualize(rids_c);
        prec, rec, f1 = prec_rec_f1(rids_c[:,:]);
        if f1 > MAXF1: MAXF1, MAXPR = [f1, [prec,rec]];
        #-Get the pair with the highest probability----------------------------------------------------------------
        max_pos      = np.unravel_index(np.argmax(scores),scores.shape);
        max_val      = scores[max_pos];
        keep, remove = [[max_pos[0]],[max_pos[1]]] if max_pos[0]<max_pos[1] else [[max_pos[1]],[max_pos[0]]];
        #-Merge the clusters or terminate--------------------------------------------------------------------------
        if max_val < threshold: break;
        if max_val < min_sim: min_sim = max_val;
        if f1 > max_f1:
            max_f1 = f1;
            size_l,size_r = MC[:,max_pos].sum(0);
            cur_out.execute("INSERT INTO tuning VALUES(?,?,?,?,?,?)",(stamp,min_sim,max_f1,N,size_l,size_r,));
        #if string != old_string:
        #    print string; old_string = string;
        #    print '--------------------------------------------------------------------------------------';
        #    print 'P:',int(round(prec,2)*100),'F:',int(round(f1,2)*100),'R:',int(round(rec,2)*100),'|',N,'|',C,'|',MC[:,max_pos[0]].sum(),'+',MC[:,max_pos[1]].sum(),'|',int(rids_c[max_pos[0],1:].sum()),'+',int(rids_c[max_pos[1],1:].sum()),'|', max_val, '>=', threshold;
        #    print '--------------------------------------------------------------------------------------';
        C           -= len(remove);
        MC[:,keep]  += MC[:,remove];
        MC[:,remove] = 0;
        #-Update the iteration-dependent components----------------------------------------------------------------
        f_C[:,keep,:]    += f_C[:,remove,:];
        f_C_C[:,keep,:]  += f_C_C[:,remove,:];
        f_C_C[keep,:,:]  += f_C_C[remove,:,:];
        f_C_C[:,remove,:] = 0;
        f_C_C[remove,:,:] = 0;
        p_C_C[:,remove,:] = 0;
        p_C_C[remove,:,:] = 0;
        p_C_C[:,keep,:]   = (f_C_C[:,keep,:] / f_C[:,keep,:]);
        p_C_C[keep,:,:]   = (f_C_C[keep,:,:] / f_C[:,:,:]);
        scores[:,keep]    = (p_C_C[:,keep,:].sum(2)) / len(ftypes);
        scores[keep,:]    = (p_C_C[keep,:,:].sum(2)) / len(ftypes);
        scores[:,remove]  = 0;
        scores[remove,:]  = 0;
        scores[keep,keep] = 0;
        #print 'scores__________________________________________________________________________'; print scores; print '________________________________________________________________________________';
        #----------------------------------------------------------------------------------------------------------
    rids_c = csr(MC).T.dot(D.MR_[local,:]);
    #string = visualize(rids_c);
    prec, rec, f1 = prec_rec_f1(rids_c[:,:]);
    if f1 > MAXF1: MAXF1, MAXPR = [f1, [prec,rec]];
    #print string;
    #print '--------------------------------------------------------------------------------------';
    #print 'P:',int(round(prec,2)*100),'F:',int(round(f1,2)*100),'R:',int(round(rec,2)*100),'|',N,'|',C,'|';
    #print '--------------------------------------------------------------------------------------';
    #-Do the remaining standard operations-------------------------------------------------------------------------
    labelling_loc         = MC.nonzero()[1]; # Since we use unique, it shouldn't matter that some cluster-indices are unassigned
    labelling_new         = labelling_loc+D.n; #print labelling_loc; print labelling_new;
    D.labelling[local]    = labelling_new;
    D.labels, D.labelling = np.unique(D.labelling,return_inverse=True);
    D.n                   = len(D.labels); #print 'Made', len(set(labelling_loc)), 'clusters.';
    #--------------------------------------------------------------------------------------------------------------
    #print '--------------------------------------------------------------------------------------\n--------------------------------------------------------------------------------------';
    return D;

def clusterer(D):
    global TIME, SIZE, CLUS, COMP;
    t_clu     = time.time()
    new_nodes = D.new.nonzero()[0];
    for new_node in new_nodes:
        mentions        = D.NM[new_node,:].nonzero()[1];
        SIZE            = max(SIZE,len(mentions));
        COMP[mentions] += 1; #TODO: reset
        print('Clustering the new node', D.index2node[new_node], 'with', len(mentions), 'mentions');#'with mentions',[D.index2mentionID[mentionIDIndex] for mentionIDIndex in mentions];
        if len(mentions) > 0 and len(mentions) <= _cfg['max_size']:
            #print _p_.memory_info().rss/_mb_, 'MB used';
            D = DBSCAN(D,mentions) if _dbscan_ else AGGLO(D,mentions);
            #D = DBSCAN_SKLEARN(D,mentions);
            #D = AGGLO(D,mentions);
    D.MC      = csr((np.ones(len(D.labelling),bool),(np.arange(len(D.labelling)),D.labelling)),shape=(len(D.labelling),len(D.labels)),dtype=bool);
    D.NC      = D.NM.dot(D.MC);
    D.rids_c  = D.MC.T.dot(D.MR_);
    D.new     = np.zeros(D.new.shape,dtype=bool);
    TIME[CLU] = time.time() - t_clu; CLUS = len(new_nodes);
    return D;

def merge(D,group):
    global MEM;
    r        = group[np.argmax([len(D.nodes[D.index2node[i]][REP]) for i in group])];                                                                               mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    #r        = group[np.argmax([D.NM[i,:].sum() for i in group])];
    #print 'merging', [D.index2node[i] for i in group], 'into', D.index2node[r];                      mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    remove   = [x for i,x in enumerate(group) if x!=r];                                                                                                             mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    keep     = sorted(list(set(range(D.spec.shape[0]))-set(remove)));  #print remove, keep;                                                                          mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    D.edge   = combine(D.edge,group,r,keep,False,True);                                                                                                                  mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    old_size = D.NM[r,:].sum(); #print '$$$ OLD SIZE:',old_size,'$$$';
    D.NM     = combine(D.NM,group,r,keep,False,False);                                                                                                                    mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    new_size = D.NM[keep.index(r),:].sum(); #print '$$$ NEW SIZE:',new_size,'$$$', D.NM.shape,D.MR_.shape;
    D.rids_b = D.NM.dot(D.MR_);                                                                                                                                     mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    D.spec   = combine(D.spec,group,r,keep,True,True);                                                                                                                   mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    D.obs    = combine(D.obs,group,r,keep,False,False);                                                                                                                   mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    D.car    = (D.obs.T.dot(D.spec.T)).T;                                                                                                                           mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    D.obs_   = combine(D.obs_,group,r,keep,False,False);                                                                                                                   mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    D.car_   = (D.obs_.T.dot(D.spec.T)).T;
    D.weight = D.car.T.multiply(D.edge).multiply(csr(1./D.car.toarray(),shape=D.car.shape,dtype=float));                                                            mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    if _old_scipy_:
        D.weight = set_diagonal(D.weight,D.obs/D.car);
    else:
        D.weight = set_diagonal(D.weight,csr(D.obs/D.car,shape=D.obs.shape));                                                                                       mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    D.weight.eliminate_zeros();                                                                                                                                     mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    D.nodes[D.index2node[r]][RID] = sum([D.nodes[D.index2node[i]][RID] for i in group],Counter()); #TODO: Check if still required
    D.nodes[D.index2node[r]][PTS] = set().union(*[D.nodes[D.index2node[i]][PTS] for i in group])
    D.nodes[D.index2node[r]][REP] = set().union(*[D.nodes[D.index2node[i]][REP] for i in group]) if _slot_merge or (not _licensing) or match([D.nodes[D.index2node[i]][REP] for i in group]) else set().intersection(*[D.nodes[D.index2node[i]][REP] for i in group]);
    D.nodes[D.index2node[r]][STR] = string(D.nodes[D.index2node[r]][REP]);
    mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    for i in remove: D.nodes[D.index2node[i]][RID] = Counter();                                    #TODO: Check if still required
    mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    D.update_index(keep,r,old_size!=new_size);                                                                                                                      mem=_p_.memory_info().rss/_mb_; MEM[MER]=max(MEM[MER],mem);
    #D.nodes[D.index2node[r]][SPE] = set([D.index2node[node_index] for node_index in D.edge[r,:].nonzero()[1]]);
    #D.nodes[D.index2node[r]][GEN] = set([D.index2node[node_index] for node_index in D.edge[:,r].nonzero()[0]]);
    return D;

def get_groups(D,thr):
    nm      = np.array(D.NM.sum(1)).astype(float);
    ok_slot = get_ok_slot(D) if _slot_merge else D.edge#csr(np.ones(D.edge.shape,dtype=bool),dtype=bool);
    weight  = D.weight.multiply(D.weight.diagonal()[:,None]) if _weight_self else D.weight;
    if _random_:
        ok_size = nm+nm.T <= _cfg['max_size']; #TODO: This could cause an inacceptable deviation for the random baselines!!!
        edges   = D.edge.multiply(ok_size).multiply(ok_slot).toarray();
        np.fill_diagonal(edges,False);
        if _nbrdm_:
            if _p_new_:
                p_new = 1-((nm**2+nm.T**2)/((nm+nm.T)**2)); p_new[np.isnan(p_new)]=0.5; p_new[p_new==0]=0.5; np.fill_diagonal(p_new,0.0);
                rows,cols = np.argwhere(p_new==np.amax(p_new)).T;
            else:
                rows,cols = edges.nonzero();
        else:
            labels    = connected_components(edges)[1];
            rows,cols = np.triu(labels[:,None]==labels).nonzero();
        selects     = np.random.choice(list(range(len(rows))),min(len(rows),_cfg['num_rdm']),replace=False) if len(rows)>0 else [];
        rows_,cols_ = rows[selects], cols[selects];
        to_merge    = csr((np.ones(len(rows_),bool),(rows_,cols_)),shape=edges.shape);
    elif _cfg['shortcut']:
        to_merge = D.edge;
    else:
        if _p_new_:
            p_new                  = 1-((nm**2+nm.T**2)/((nm+nm.T)**2));
            p_new[np.isnan(p_new)] = 0.5;
            p_new[p_new==0]        = 0.5;
            np.fill_diagonal(p_new,0.0);
            score                  = weight.multiply(p_new*2); #The *2 is only such that the optimum of two equal sized blocks gets 1-weighting
        else:
            score = weight;
        kth      = np.argsort(score.data)[-min(score.data.size,_top_k_)] if _top_k_ != None else None;
        thr      = max(0.0000000001,score.data[kth]) if _top_k_ != None else thr; #TODO: if _top_k need to do -min_value
        to_merge = ok_slot.multiply(score > thr); #TODO: >= is inefficient, do >
    labels    = connected_components(to_merge)[1];
    sorting   = np.argsort(labels);
    labels_s  = labels[sorting];
    _, starts = np.unique(labels_s,return_index=True);
    sizes     = np.diff(starts);
    groups    = [group for group in np.split(sorting,starts[1:]) if group.size > 1];
    return groups;

def merger(D,thr):
    global TIME, MERGE, BOUND;
    mer_t  = time.time();
    groups = get_groups(D,thr);
    groups = [[D.index2node[i] for i in group] for group in groups];  #TODO: Keep the node names, because the indices are changing!
    number = 1;
    for group in groups:
        if number%1000==0: print('...........................',number*100/float(len(groups)),'% merged');
        group_idx = [D.node2index[name] for name in group];
        if D.NM[group_idx,:].sum() <= _cfg['max_size']:
            D = merge(D,group_idx); #print _p_.memory_info().rss/_mb_, 'MB used';
        else:
            BOUND += 1;
            print('group union too large!');
        number+=1;
    D.edge = transitive_reduction(D.edge); #TODO: This is new!
    TIME[MER] = time.time() - mer_t; MERGE = len(groups);
    return D;

def discounter(D,d):
    global TIME, MEM;
    dis_t    = time.time();
    O_dense  = D.obs.toarray();                                                                         mem=_p_.memory_info().rss/_mb_; MEM[DIS]=max(MEM[DIS],mem); #print mem,'MB used';
    distrib  = D.weight.multiply(csr(1./D.weight.sum(1),shape=(D.weight.shape[0],1),dtype=float));      mem=_p_.memory_info().rss/_mb_; MEM[DIS]=max(MEM[DIS],mem); #print mem,'MB used';
    discount = D.obs*d;                                                                                 mem=_p_.memory_info().rss/_mb_; MEM[DIS]=max(MEM[DIS],mem); #print mem,'MB used';
    gain     = (discount.T.dot(distrib)).T;                                                             mem=_p_.memory_info().rss/_mb_; MEM[DIS]=max(MEM[DIS],mem); #print mem,'MB used';
    O_dense -= discount.toarray();                                                                      mem=_p_.memory_info().rss/_mb_; MEM[DIS]=max(MEM[DIS],mem); #print mem,'MB used';
    O_dense += gain.toarray();                                                                          mem=_p_.memory_info().rss/_mb_; MEM[DIS]=max(MEM[DIS],mem); #print mem,'MB used';
    D.obs    = csr(O_dense,shape=D.obs.shape,dtype=float);                                              mem=_p_.memory_info().rss/_mb_; MEM[DIS]=max(MEM[DIS],mem); #print mem,'MB used';
    D.car    = (D.obs.T.dot(D.spec.T)).T;                                                               mem=_p_.memory_info().rss/_mb_; MEM[DIS]=max(MEM[DIS],mem); #print mem,'MB used';
    D.weight = D.car.T.multiply(D.edge).multiply(csr(1./D.car.toarray(),shape=D.car.shape,dtype=float));mem=_p_.memory_info().rss/_mb_; MEM[DIS]=max(MEM[DIS],mem); #print mem,'MB used';
    if _old_scipy_:
        D.weight.setdiag(D.obs.toarray()/D.car.toarray());                                              mem=_p_.memory_info().rss/_mb_; MEM[DIS]=max(MEM[DIS],mem); #print mem,'MB used';
    else:
        D.weight.setdiag(np.ravel(D.obs/D.car));                                                        mem=_p_.memory_info().rss/_mb_; MEM[DIS]=max(MEM[DIS],mem); #print mem,'MB used';
    D.weight.eliminate_zeros();                                                                         mem=_p_.memory_info().rss/_mb_; MEM[DIS]=max(MEM[DIS],mem); #print mem,'MB used';
    TIME[DIS] = time.time() - dis_t;
    return D;
#-------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS-INTERFACE-----------------------------------------------------------------------------------------------------------------

def samples(D,same_file,diff_file):
    same_rid = [D.MR_[:,:][:,i].nonzero()[0] for i in range(D.MR_[:,:].shape[1])];
    num_same = sum([(len(el)**2) for el in same_rid]);
    diff_rid = [];
    num_diff = 0;
    for i in range(len(same_rid)):
        if num_diff <= num_same and (same_rid[:i]!=[] or same_rid[i+1:]!=[]):
            diff_rid.append([same_rid[i],np.concatenate(same_rid[:i]+same_rid[i+1:])]);
            num_diff += len(diff_rid[-1][0])*len(diff_rid[-1][0]);
    tmp_sames = [sim(same_rid[i],D,same_rid[i]) for i in range(len(same_rid))];
    tmp_sames = [tmp_sames[i][(np.triu(tmp_sames[i],1)+np.tril(tmp_sames[i],-1)).nonzero()] for i in range(len(same_rid))];
    tmp_diffs = [np.ravel(sim(diff_rid[i][0],D,diff_rid[i][1])) for i in range(len(diff_rid))];
    similarities_same = np.concatenate( tmp_sames );
    similarities_diff = np.concatenate( tmp_diffs )[:len(similarities_same)];
    print(similarities_same); print(similarities_diff);
    OUT=open(same_file,'a'); OUT.write('\n'.join([str(similarities_same[i]) for i in range(len(similarities_same))])+'\n'); OUT.close();
    OUT=open(diff_file,'a'); OUT.write('\n'.join([str(similarities_diff[i]) for i in range(len(similarities_diff))])+'\n'); OUT.close();

def progress(D,t_start,con_out,cur_out,end=0.0):
    global iteration, COMP, _job_id;
    base_prec, base_rec, base_f1 = prec_rec_f1(csr(D.MR_[:,:].sum(0))); print('basePrec:', base_prec, 'baseRec:', base_rec, 'base_f1:', base_f1);
    I = 0; B = str(_key)+', '+_value if _value!=None else str(_key); COMP = np.zeros(D.NM.shape[1],dtype=int); c_time_0 = time.time();
    thr_iter = _cfg['thr']*[1,_cfg['selfprob_fac']][_weight_self]; #+0.000000000001TODO: Undo the addition if required
    if _cfg['do_results']: output(D,I,B,t_start,0,time.time()-c_time_0,thr_iter,con_out,cur_out);
    log_avg_repsize = np.log(sum([len(D.nodes[node][REP])*D.NM[D.node2index[node],:].sum() for node in D.index2node])/D.NM.sum());
    while thr_iter > end:
        thr_iter -= _cfg['step']*[1,_cfg['selfprob_fac']][_weight_self]; m_time_0 = time.time(); print('I =',I,'| t =',thr_iter, '| log avg rep size =', log_avg_repsize);# print len(D.index2node);
        #if I == 0:
        #    old_job_id = _job_id; _job_id = _job_id+'_init';
        #    if _cfg['do_json']: tojson(D,I);
        #    if _cfg['do_graph']: draw(D,colors,I,False);
        #    if _cfg['do_tree']: draw(D,colors,I,True);
        #    if _cfg['do_equiDB']: equiDB(D,I);
        #    _job_id = old_job_id;
        D = merger(D,thr_iter) if log_avg_repsize < _repsize_thr else D;
        #complete_reps(D,False,False);
        #complete_reps(D,True,False);
        #if I==99: complete_reps(D,True,True);
        #if I > -1: tojson(D,I); draw(D,colors,I,False); draw(D,colors,I,True); #tojson(D,I)
        if I in [0,1,2,3,4,5,6,7,8,9,10,15,20,15,30,35,40,45,50,55,60,65,70,75,80,85,90,95,99,100]:
            if _cfg['do_json']: tojson(D,I);
            if _cfg['do_graph']: draw(D,colors,I,False);
            if _cfg['do_tree']: draw(D,colors,I,True);
            if _cfg['do_equiDB']: equiDB(D,I);
        D      = discounter(D,_d_) if log_avg_repsize < _repsize_thr else D;
        m_time = time.time() - m_time_0; c_time_0 = time.time();
        c_time = time.time() - c_time_0;
        I += 1;
        log_avg_repsize = np.log(sum([len(D.nodes[node][REP])*D.NM[D.node2index[node],:].sum() for node in D.index2node])/D.NM.sum());
        if _cfg['do_results']: output(D,I,B,t_start,m_time,c_time,thr_iter,con_out,cur_out);

def merge_all_iteratively(D,t_start,con_out,cur_out,end=0.0):
    global iteration, COMP, _job_id;
    base_prec, base_rec, base_f1 = prec_rec_f1(csr(D.MR_[:,:].sum(0))); print('basePrec:', base_prec, 'baseRec:', base_rec, 'base_f1:', base_f1);
    I = 0; B = str(_key)+', '+_value if _value!=None else str(_key); COMP = np.zeros(D.NM.shape[1],dtype=int); c_time_0 = time.time();
    thr_iter = _cfg['thr']*[1,_cfg['selfprob_fac']][_weight_self]; #+0.000000000001TODO: Undo the addition if required
    if _cfg['do_results']: output(D,I,B,t_start,0,time.time()-c_time_0,thr_iter,con_out,cur_out);
    log_avg_repsize = np.log(sum([len(D.nodes[node][REP])*D.NM[D.node2index[node],:].sum() for node in D.index2node])/D.NM.sum());
    detail_node     = D.index2node[0];
    while thr_iter > end:
        thr_iter -= _cfg['step']*[1,_cfg['selfprob_fac']][_weight_self]; m_time_0 = time.time(); print('I =',I,'| t =',thr_iter, '| log avg rep size =', log_avg_repsize);# print len(D.index2node);
        D = merger(D,thr_iter) if log_avg_repsize < _repsize_thr else D;
        D      = discounter(D,_d_) if log_avg_repsize < _repsize_thr else D;
        m_time = time.time() - m_time_0; c_time_0 = time.time();
        c_time = time.time() - c_time_0;
        I += 1;
        if _cfg['do_results']: output(D,I,B,t_start,m_time,c_time,thr_iter,con_out,cur_out);
        if _cfg['do_json']: tojson(D,I);
        if _cfg['do_graph']:
            draw(D,colors,I,False);
            if detail_node in D.node2index: # Can only plot this node as long as it exists
                draw_one_context(D,colors,D.node2index[detail_node],I,False);
        if _cfg['do_tree']: draw(D,colors,I,True);
        if _cfg['do_equiDB']: equiDB(D,I);

def interface(D,colors):
    global iteration, COMP;
    old_D = copy(D); old_iteration = iteration; I = 0; B = _cfg['root_dir']+_cfg['name_db'];
    COMP  = np.zeros(D.NM.shape[1],dtype=int);
    while True:#(_cfg['thr']*[1,_cfg['selfprob_fac']][_weight_self])-(I*_cfg['step']*[1,_cfg['selfprob_fac']][_weight_self]) >= 0:
        sanity_check(D);
        print('t =',(_cfg['thr']*[1,_cfg['selfprob_fac']][_weight_self])-(I*_cfg['step']*[1,_cfg['selfprob_fac']][_weight_self])); print(len(D.index2node)); #draw(D,colors,0);
        option=input("... m(erge) - h(ypothesize) - d(iscount) - c(luster) - r(eset) - p(lot) - j(son) ...");
        if option=='m':   #-MERGE------------------------------------------------------------------------------------------------------------
            old_D = copy(D); old_iteration = iteration;
            D     = merger(D,(_cfg['thr'])-(I*_cfg['step']*[1,_cfg['selfprob_fac']][_weight_self]));   print('---------------------------------------------------------------done merging.');
            I    += 1;
        elif option=='h': #-HYPOTHESES-------------------------------------------------------------------------------------------------------
            old_D = copy(D); old_iteration = iteration;
            D     = hypothesizer(D,0.3);                        print('---------------------------------------------------------------done hypothesizing.');
        elif option=='d': #-DISCOUNT---------------------------------------------------------------------------------------------------------
            old_D = copy(D); old_iteration = iteration;
            D     = discounter(D,1.0);                          print('---------------------------------------------------------------done discounting.');
        elif option=='c': #-CLUSTER----------------------------------------------------------------------------------------------------------
            old_D = copy(D); old_iteration = iteration;
            D     = clusterer(D);                               print('---------------------------------------------------------------done clustering.');
        elif option=='r': #-RESET------------------------------------------------------------------------------------------------------------
            D = old_D; iteration = old_iteration;               print('---------------------------------------------------------------done resetting.');
        elif option=='p': #-PLOT-------------------------------------------------------------------------------------------------------------
            draw(D,colors,0,False); draw(D,colors,0,True);      print('---------------------------------------------------------------done plotting.');
        elif option=='j': #-RESET------------------------------------------------------------------------------------------------------------
            tojson(D,0);                                        print('---------------------------------------------------------------done jsoning.');
        else:
            print('No such option.');
#-------------------------------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#SCRIPT---------------------------------------------------------------------------------------------------------------------------------------------------------------------
#TODO: For some strange reason, in very large subsets, sometimes the cover counts are a little bit off... #TODO: "R Quian Quiroga" is observed 10 times if the Q not Quiroga_R is used (it says 8 in both)
#-------------------------------------------------------------------------------------------------------------------------------------
_constraints = load_constraints(_cfg['con_file']);
#-------------------------------------------------------------------------------------------------------------------------------------
if _cfg['mode'] != 'sample':
    con_out = sqlite3.connect(_result_db);
    cur_out = con_out.cursor();
    if _checker_:
        new_db = cur_out.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='results'").fetchall() == []; print('new_db:', new_db);
        if not new_db:
            num_rows = cur_out.execute("SELECT count(*) FROM results").fetchall()[0][0]; print('num_rows:', num_rows);
            if num_rows==402: exit('This has already been calculated. Skipping...');
            cur_out.execute("DROP TABLE IF EXISTS results"); cur_out.execute("DROP TABLE IF EXISTS tuning");
    cur_out.execute("DROP TABLE IF EXISTS results"); cur_out.execute("DROP TABLE IF EXISTS tuning");
    cur_out.execute("CREATE TABLE IF NOT EXISTS results(t_start TEXT, t_iter TEXT, bottom TEXT, eps REAL, z REAL, r REAL, minPts INT, smooth REAL, iteration INTEGER, num_m INTEGER, num_r INTEGER, merge INT, clus INT, comp REAL, blocksum INT, bound INT, num_b INTEGER, pre_b REAL, rec_b REAL, f1_b REAL, tp_b REAL, t_b REAL, p_b REAL, num_c INTEGER, pre_c REAL, rec_c REAL, f1_c REAL, tp_c REAL, t_c REAL, p_c REAL, max_p REAL, max_r REAL, max_f1 REAL, max_size INT, mem_clu INT, mem_mer INT, mem_dis INT, time_clu REAL, time_sim REAL, time_alg REAL, time_mer REAL, time_dis REAL, cpu_m REAL, cpu_c REAL, num_nodes_start INT, num_oversize INT, num_nodes_rel INT, gini_repsize_start INT, gini_reps INT, gini_reps_rel INT, gini_cross_weight INT, gini_repsize_start_unw INT, gini_reps_unw INT, gini_reps_rel_unw INT, node_gini INT, node_gini_unw INT, node_gini_cross INT, sum_oversize INT, reps_x_ment INT)");
    cur_out.execute("CREATE TABLE IF NOT EXISTS tuning(stamp TEXT, min_sim REAL, max_f1 REAL, size INT, left INT, right INT)");
#-------------------------------------------------------------------------------------------------------------------------------------
print('Building graph...'); t = time.time();
#-------------------------------------------------------------------------------------------------------------------------------------
#lat_cur    = load_lattice(_cfg['lat_file']);
node_infos = load_node_infos_db(_cfg['root_dir']+_cfg['name_db'],_key,_value,_typeonly);
#-------------------------------------------------------------------------------------------------------------------------------------
print(time.time()-t, 'seconds for loading data.'); t = time.time();
#-------------------------------------------------------------------------------------------------------------------------------------
observed_nodes = [make_node(node_info,_cfg['aggregate']) for node_info in node_infos if list(node_info[1].keys())!=[None] or not _cfg['only_rIDs']];
illegal_nodes  = [observed_node for observed_node in observed_nodes if not licenced(observed_node[TYP])];
observed_nodes = [observed_node for observed_node in observed_nodes if     licenced(observed_node[TYP])];
mentions       = [(node[REP],rID,1.0,None,) for node in observed_nodes for rID in node[RID] for i in range(int(node[RID][rID]))] if _cfg['aggregate'] else [(node[REP],rID,1.0,mentionID,) for node in observed_nodes for rID in node[RID] for mentionID in node[RID][rID]];
rIDs           = sorted(list(set([rID for i in range(len(observed_nodes)) for rID in observed_nodes[i][RID]])));
#-------------------------------------------------------------------------------------------------------------------------------------
print('Number of mentionIDs:', len(mentions), 'Number of rIDs:', len(rIDs));
if len(rIDs) == 0: exit();
print('First rID is', rIDs[0]);#sort so that None is up front
print('-ILLEGAL-REPRESENTATIONS-------------------------------------------------------------------------');
for illegal_node in illegal_nodes:
    print(illegal_node[STR]); print('--------------------------------------------------------------------------------------');
print('-------------------------------------------------------------------------------------------------');
#-------------------------------------------------------------------------------------------------------------------------------------
colorscheme = get_colors(len(rIDs)+0)[0:];
colors      = {i:colorscheme[i] for i in range(len(rIDs))}; colors[0] = (0.,0.,1.) if rIDs[0]==None else colors[0]; #white is for None
Nodes       = dict();
iteration   = 0;
#-------------------------------------------------------------------------------------------------------------------------------------
print(time.time()-t, 'seconds for preprocessing.'); t = time.time();
#-------------------------------------------------------------------------------------------------------------------------------------
#ID2rep  = {node[STR]:node[REP] for node in observed_nodes}
#_minels = find_min_els(ID2rep.keys(),ID2rep);
#for minel in _minels:
#    print minel;
#-------------------------------------------------------------------------------------------------------------------------------------
#print time.time()-t, 'seconds for finding minimum elements.'; t = time.time();
#-------------------------------------------------------------------------------------------------------------------------------------
if _clean_all_nodes and _clean_without_exception: #Just producing the partial order over the observed nodes without unobserved intermediate nodes
    node_reps           = [node[REP] for node in observed_nodes];
    SPES,GENS,SP_s,GE_s = [find_edges_spe(node_reps),find_edges_gen(node_reps)][_find_edges_gen];
    for i in range(len(observed_nodes)):
        obs = observed_nodes[i][OBS];
        car = (sum([observed_nodes[j][OBS] for j in SP_s[i]]) if i in SP_s else 0      ) + obs;
        spe =  set([observed_nodes[j][STR] for j in SPES[i]]) if i in SPES else set([]);
        gen =  set([observed_nodes[j][STR] for j in GENS[i]]) if i in GENS else set([]);
        rep = node_reps[i];
        nid = observed_nodes[i][STR];
        mod = 0;
        rid = observed_nodes[i][RID];
        sp_ = (set([observed_nodes[j][STR] for j in SP_s[i]]) if i in SP_s else set([])) | set([]); #TODO: Do the SP_ include self?
        typ = get_type(rep);
        pts = set([nid]);
        Nodes[nid] = [obs,car,spe,gen,rep,nid,mod,rid,sp_,typ,pts];
else:
    for i in range(len(observed_nodes)):
        if list(observed_nodes[i][RID].keys()) != []:
            add_node(observed_nodes[i][REP],observed_nodes[i][RID],_cfg['aggregate'],Nodes);
    if _clean_all_nodes: #Meaning that _clean_without_execption==False
        Nodes = clean_all_nodes(Nodes);
    #Nodes = clean_nodes(Nodes); #This should be taken care of by using the minimum elements
#get_slot_statistics(Nodes);
#-------------------------------------------------------------------------------------------------------------------------------------
print(time.time()-t, 'seconds for adding nodes.'); t = time.time();
#-------------------------------------------------------------------------------------------------------------------------------------
D = DATA(Nodes,mentions,rIDs,False,_cfg['aggregate']);
#-------------------------------------------------------------------------------------------------------------------------------------
if _cfg['do_json'] == True: #TODO: This is not the optimal place, it would better be done in the load_node_infos_db()
    print('For JSON, loading mention-infos from disk. This is currently time inefficient...');
    con         = sqlite3.connect(_cfg['root_dir']+_cfg['name_db']);
    cur         = con.cursor();
    D.mentInfos = {D.index2node[nodeIndex]: mentioninfos(nodeIndex,D,cur) for nodeIndex in range(D.NM.shape[0])};
    D.mentInfos = {D.mentionID2index[mentionID]: D.mentInfos[nodeIndex][mentionID] for nodeIndex in D.mentInfos for mentionID in D.mentInfos[nodeIndex]};
    con.close();
#-------------------------------------------------------------------------------------------------------------------------------------
print(time.time()-t, 'seconds for making representation.'); t = time.time();
#-------------------------------------------------------------------------------------------------------------------------------------
GINI_mentions_start     = gini(D.NM.sum(1));
GINI_repsize_start      = gini([len(node[REP])*node[OBS] for node in observed_nodes]);
GINI_repsize_start_unw  = gini([len(node[REP]) for node in observed_nodes]);
NUM_NODES_start     = len(observed_nodes);#D.edge.shape[0];
node_sizes = sorted([(D.NM[i,:].sum(1),D.index2node[i]) for i in range(D.NM.shape[0])]);
for size, node_str in node_sizes:
    if size > 0: print(node_str, size);
#-------------------------------------------------------------------------------------------------------------------------------------
D_old   = copy(D);
t_start = datetime.datetime.utcnow().isoformat().replace('T',' ')[:-7];
if _cfg['mode'] == 'interface':
    interface(D,colors);
elif _cfg['mode'] == 'sample':
    samples(D,''.join(_result_db.split('.')[:-1])+'_'+_cfg['same_file'],''.join(_result_db.split('.')[:-1])+'_'+_cfg['diff_file']);
elif _cfg['mode'] == 'collocation':
    exit();
elif _cfg['mode'] == 'merge_all':
    merge_all_iteratively(D,t_start,con_out,cur_out,0.0);
else:                                    
    progress(D,t_start,con_out,cur_out,0.0);
#for key in D.nodes:
#    for el in D.nodes[key][REP]:
#        if el[0]=='division' and el[1]=='Technol':
#            print key; print D.nodes[key][REP]; print '--------------------------';
#D = copy(D_old);
# prob_for('terms',np.array(range(D.NM.shape[1])),D).sum(0) for testing normalization
#-------------------------------------------------------------------------------------------------------------------------------------
print(time.time()-t, 'seconds for main processing.'); t = time.time();
#-------------------------------------------------------------------------------------------------------------------------------------
'''
print 'Number of nodes at the beginning:', len(Nodes);
Nodes = dict(); iteration = 0;
for node in D.nodes:
    if D.nodes[node][RID].keys() != []:
        add_node(D.nodes[node][REP],D.nodes[node][RID],True,Nodes);
#Nodes = clean_nodes(Nodes);
#Nodes = clean_all_nodes(Nodes);
print 'Number of nodes at the 2nd beginning:', len(Nodes);
mentions = [(Nodes[node][REP],rID,1.0,None,) for node in Nodes for rID in Nodes[node][RID] for i in xrange(int(Nodes[node][RID][rID]))]
rIDs     = sorted(list(set([rID for node in Nodes for rID in Nodes[node][RID]])));
_cfg['do_json'] = False; _cfg['do_equiDB'] = False; #TODO: Can be removed if a way is found to recover the mentionIDs
_job_id = _job_id+'_2nd';
_allow_complete_merges = False;
D = DATA(Nodes,mentions,rIDs,False,True);
progress(D,t_start,con_out,cur_out,0.0);
'''
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
