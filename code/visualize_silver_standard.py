import sqlite3
import sys
import numpy as np
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components as components

_silver    = 'resources/dfg_wos_silver_standard.db';
_hierarchy = 'resources/DFG_generalizations_dfg.db';
_outfolder = 'output/dfg/';
_pair_db   = 'output/relations_true';

_numbers = True;

con_silver = sqlite3.connect(_silver);
cur_silver = con_silver.cursor();
con_hier   = sqlite3.connect(_hierarchy);
cur_hier   = con_hier.cursor();

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

def transitive_closure(M):
    edges     = set_diagonal(M,csr(np.zeros(M.shape[0],dtype=bool)[:,None]));
    closure   = edges.copy();
    num, i    = 1,2;
    while num > 0 and i <= 20: #TODO: the smaller should not be required but it seems that sometimes there are cycles in the graph
        print('...',i,':',num);
        new        = edges**i;
        num        = len(new.nonzero()[0]);
        closure    = closure + new;
        i         += 1;
        closure.eliminate_zeros();
        if closure.diagonal().sum() > 0:
            print('WARNING: Cycles in input matrix!');
    return set_diagonal(closure,csr(M.diagonal()[:,None])).astype(bool);

def make_node(dfg_id,cur):
    rows         = cur.execute("SELECT DISTINCT dfg_de FROM mapping WHERE dfgid=? and verified1=1",(str(dfg_id),)).fetchall();
    header, rest = [rows[0][0],'\\n---------------------------'] if len(rows) > 0 else [[cur_hier.execute("SELECT de FROM translations WHERE dfgid=?",(dfg_id,)).fetchall()[0][0].replace(' ','\\n'),str(dfg_id)][_numbers],None];
    if rest != None: # There are some mapped affiliation strings
        rows = cur.execute("SELECT DISTINCT ref_string FROM mapping WHERE dfgid=? and verified1=1",(str(dfg_id),)).fetchall();
        for row in rows:
            rest  += '\\n'+row[0];
    else: # There are no mapped affiliation strings
        rest = '';
    return '"'+header+rest+'"';


dfgid2index = dict();
index2dfgid = [];

for row in cur_hier.execute("SELECT DISTINCT dfgid FROM hierarchy"):
    dfgid2index[row[0]] = len(index2dfgid);
    index2dfgid.append(row[0]);
for row in cur_hier.execute("SELECT DISTINCT parent FROM hierarchy"):
    if not row[0] in dfgid2index:
        dfgid2index[row[0]] = len(index2dfgid);
        index2dfgid.append(row[0]);

relations = set(((dfgid2index[parent],dfgid2index[dfg_id]) for dfg_id,parent in cur_hier.execute("SELECT dfgid,parent FROM hierarchy WHERE parent is not NULL")));
rows,cols = zip(*relations);

forest      = csr((np.ones(len(rows),dtype=bool),(rows,cols)),dtype=bool,shape=(max(rows+cols)+1,max(rows+cols)+1));
num, labels = components(forest);

index2verdfg, index2mentionID = zip(*cur_silver.execute("SELECT dfgid,mentionID FROM mapping WHERE verified1=1").fetchall());
mentionID2index               = {index2mentionID[i]:i for i in range(len(index2mentionID))};
rows                          = [dfgid2index[int(dfgid)] for dfgid in index2verdfg];
cols                          = [mentionID2index[mentionID] for mentionID in index2mentionID];
NM                            = csr((np.ones(len(rows),dtype=bool),(rows,cols)),shape=(forest.shape[0],max(cols)+1),dtype=bool);

for i in range(num): #num
    index2node = np.where(labels==i)[0];
    node2index = { index2node[j]:j for j in range(len(index2node)) };
    tree       = forest[index2node,:][:,index2node];
    root       = index2dfgid[index2node[np.where(tree.sum(0)==0)[1][0]]]; #assuming it is a tree and there is exactly one root
    rows_name  = cur_hier.execute("SELECT de FROM translations WHERE dfgid=?",(root,)).fetchall()
    if len(rows_name)==0: continue;
    root_str   = rows_name[0][0].replace(' ','_').replace('/','_');
    edges      = zip(*tree.nonzero());
    edges_i    = [(index2node[fro],index2node[to],) for fro,to in edges];
    edges_dfg  = [(index2dfgid[fro],index2dfgid[to],) for fro,to in edges_i];
    edges_str  = [(make_node(fro,cur_silver),make_node(to,cur_silver),) for fro,to in edges_dfg];
    OUT        = open(_outfolder+str(i)+'_'+str(root)+'_'+root_str+'.dot','w');
    OUT.write("digraph G {\nnode [shape=box,height=0,width=0]\n"+"\n".join((parent+' -> '+child+';' for parent,child in edges_str)));
    if _numbers: OUT.write("\n"+"\n".join(['"'+str(index2dfgid[node])+'" [label=""]' for node in index2node]));
    OUT.write("\n}");
    OUT.close();
    print(i,str(root),root_str);
    #------------------------------------------------
    equal             = NM.T[:,index2node].dot(NM[index2node,:]); equal.setdiag(False); equal.eliminate_zeros();
    equivalences      = [(int(index2mentionID[fro]),int(index2mentionID[to]),) for fro,to in zip(*equal.nonzero())];
    rep2rep           = transitive_closure(tree); rep2rep.setdiag(False); rep2rep.eliminate_zeros();
    ment2ment         = NM.T[:,index2node].dot(rep2rep).dot(NM[index2node,:]);
    supersets         = [(int(index2mentionID[fro]),int(index2mentionID[to]),) for fro,to in zip(*ment2ment.nonzero())];
    if len(equivalences)==0 and len(supersets)==0:
        continue;
    con_out = sqlite3.connect(_pair_db+'_'+root_str+'.db'); cur_out = con_out.cursor();
    cur_out.execute("DROP TABLE IF EXISTS equivalent");
    cur_out.execute("DROP TABLE IF EXISTS supersets");
    cur_out.execute("CREATE TABLE equivalent(x INT, y INT, UNIQUE(x,y))");
    cur_out.execute("CREATE TABLE supersets(x INT, y INT, UNIQUE(x,y))");
    cur_out.executemany("INSERT INTO equivalent VALUES(?,?)",equivalences); con_out.commit();
    cur_out.executemany("INSERT INTO supersets VALUES(?,?)",supersets); con_out.commit(); con_out.close();



