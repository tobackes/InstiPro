#########################################################################################################################################
# IMPORTS ###############################################################################################################################
from collections import Counter
import os,sys
import re, regex
import csv
import itertools
import functools
import operator
from copy import deepcopy as copy
import sqlite3
import numpy as np
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components
from langdetect import detect
import multiprocessing as MP
import time
import math
#########################################################################################################################################
# GLOBAL OBJECTS ########################################################################################################################

infolder = sys.argv[1];
#freqDB  = sys.argv[2];
phrfile = sys.argv[2];
trafile = sys.argv[3];
modfile = sys.argv[4];

CNT = 0; TRE = 1; PRO = 2;

_langs = ['af','ar','bg','bn','ca','cs','cy','da','de','el','en','es','et','fa','fi','fr','gu','he','hi','hr','hu','id','it','ja','kn','ko','lt','lv',
          'mk','ml','mr','ne','nl','no','pa','pl','pt','ro','ru','sk','sl','so','sq','sv','sw','ta','te','th','tl','tr','uk','ur','vi','zh-cn','zh-tw',None];

_max_len_ = 4;
_n_       = 3;
_jobs     = 64;
_jobs2    = 64;
_batch    = 10000;
_batch2   = 10000;

WORD  = re.compile(r'(\b[^\s]+\b)');
CHAR  = re.compile(r'([A-Za-z]|ß|ö|ü|ä)+');
LEGAL = regex.compile(r'\p{L}+')

#########################################################################################################################################
# FUNCTIONS #############################################################################################################################

def ngrams(seq,n):
    return [tuple(seq[i-n:i]) for i in range(n,len(seq)+1) ];

def probs_leq(p,c,tree):
    return [(w,w_) for w in tree for w_ in tree[w][TRE] if tree[w][TRE][w_][PRO]>=p and tree[w][PRO]>=c];

def make_tree(d):
    tree = dict();
    for ngram in d:
        if len(ngram) == 2:
            if ngram[0] in tree:
                tree[ngram[0]][CNT] += d[ngram];
                if ngram[1] in tree[ngram[0]][TRE]:
                    tree[ngram[0]][TRE][ngram[1]][CNT] += d[ngram];
                else:
                    tree[ngram[0]][TRE][ngram[1]] = [d[ngram],dict(),0];
            else:
                tree[ngram[0]] = [d[ngram],{ngram[1]:[d[ngram],dict(),0]},0];
    divisor = float(sum([tree[w][CNT] for w in tree]));
    for w in tree:
        tree[w][PRO] = tree[w][CNT] / divisor;
        for w_ in tree[w][TRE]:
            tree[w][TRE][w_][PRO] = float(tree[w][TRE][w_][CNT]) / tree[w][CNT];
    return tree;

def make_tree_(d):
    tree = [0,dict(),1];
    for ngram in d:
        freq        = d[ngram];
        current     = tree;
        current[0] += freq;
        for term in ngram:
            if not term in current[1]:
                current[1][term] = [0,dict(),None];
            current[1][term][0] += d[ngram];
            current              = current[1][term];
    current = tree;
    add_probs(tree[1],tree[0]);
    return tree;

def add_probs(children,count):
    for child in children:
        children[child][2] = children[child][0] / count;
        add_probs(children[child][1],children[child][0]);

def get_entropy(path,children):
    for child in children:
        path_ = path + [child];
        entro = entropy([children[child][1][node][2] for node in children[child][1]]);
        print(path_,entro);
        get_entropy(path_,children[child][1]);

def get_splits(path,children,D,concat,thr,power):
    for child in children:
        H     = entropy([children[child][1][node][2] for node in children[child][1]]);
        score = H / len(child)**power; # If normalized entropy is above the threshold then we do NOT split
        path_ = path[:-1]+[path[-1]+child] if len(path)>=1 and concat else path+[child];
        if score > 0: print(path_,score);
        get_splits(path_,children[child][1],D,score>thr,thr,power);
        if len(path_) >= 2:
            options = [''.join(path_[:i+1]) for i in range(len(path_))]
            D[''.join(path_)] = options;

def combine_counters(counters):
    c = Counter();
    i = 0;
    for counter in counters:
        i += 1;
        if i % 50 == 0:
            print(i);
        for term in counter:
            c[term] += counter[term];
    return c;

def display(p,c,tree,inversed=False):
    if inversed:
        for w,w_ in probs_leq(p,c,tree):
            print('(inv)', w_,w, tree[w][CNT], tree[w][TRE][w_][CNT], tree[w][TRE][w_][PRO]);
    else:
        for w,w_ in probs_leq(p,c,tree):
            print('(std)', w,w_, tree[w][CNT], tree[w][TRE][w_][CNT], tree[w][TRE][w_][PRO]);

def transitive_closure(M): # WARNING: Not for large M!
    labels  = connected_components(M)[1];
    closure = csr(labels==labels[:,None]);
    return closure;

def term_transitions(replace,DIST='damerau'):
    index2term = list(set([item for item in replace.keys()]) | set([item for item in replace.values()]));
    term2index = {index2term[i]:i for i in range(len(index2term))};
    rows, cols = zip(*[[term2index[item[0]],term2index[item[1]]] for item in replace.items()]);
    R              = csr((np.ones(2*len(rows)),(rows+cols,cols+rows)),dtype=bool,shape=(len(index2term),len(index2term)));
    labels         = connected_components(R)[1];
    sorting        = np.argsort(labels);
    labels_s       = labels[sorting];
    _, starts      = np.unique(labels_s,return_index=True);
    sizes          = np.diff(starts);
    groups         = [group for group in np.split(sorting,starts[1:]) if group.size > 1];
    transition     = dict();
    for group in groups:
        sum_group = float(sum([d[(index2term[index],)] for index in group]));
        max_index = None;
        max_freq  = 0;
        for index in group:
            predict_term = index2term[index];
            predict_freq = d[(predict_term,)];
            if predict_freq > max_freq:
                max_freq  = predict_freq;
                max_index = index;
        for index1 in group:
            given_term             = index2term[index1];
            len_1                  = len(given_term);
            transition[given_term] = dict();
            for index2 in [index1,max_index]:
                predict_term                         = index2term[index2];
                len_2                                = len(predict_term);
                sim_prefix                           = prefix_normed(     given_term,predict_term,len_1,len_2      );
                sim_similar                          = similarity_normed( given_term,predict_term,len_1,len_2,DIST );
                transition[given_term][predict_term] = (d[(predict_term,)]/sum_group) * sim_similar;#(sim_similar+sim_prefix)/2;
            sum_sim = sum([transition[given_term][predict_term] for predict_term in transition[given_term]]);
            for predict_term in transition[given_term]:
                transition[given_term][predict_term] /= sum_sim;
            for index2 in [index1,max_index]:
                print(given_term, '-->', index2term[index2], transition[given_term][index2term[index2]]);
    return transition;

def apply_replace(index2term,replacements):
    if len(replacements) == 0:
        return dict();
    term2index     = {index2term[i]:i for i in range(len(index2term))};
    rows,cols,sims = zip(*replacements);
    R              = csr((np.ones(2*len(rows)),(rows+cols,cols+rows)),dtype=bool,shape=(len(index2term),len(index2term)));
    labels         = connected_components(R)[1];
    sorting        = np.argsort(labels);
    labels_s       = labels[sorting];
    _, starts      = np.unique(labels_s,return_index=True);
    sizes          = np.diff(starts);
    groups         = [group for group in np.split(sorting,starts[1:]) if group.size > 1];
    replace        = dict();
    for group in groups:
        terms = [index2term[i] for i in group];
        repre = max([(d[(term,)],term) for term in terms])[1];
        for term in terms:
            if term != repre:
                replace[term] = repre;
    return replace;

def replace_by_prefix(index2term,threshold,window):
    replacements = set([]);
    for i in range(len(index2term)-window):
        len_1 =  len(index2term[i]);
        for j in range(1,window+1):
            len_2   =  len(index2term[i+j]);
            percent = prefix_normed(index2term[i],index2term[i+j],len_1,len_2);
            if percent > threshold:
                replacements.add((i+j,i,percent,));
    return replacements;

def replace_by_similar_(index2term,threshold,window,DIST):
    replacements = set([]);
    for i in range(len(index2term)-window):
        len_1     =  len(index2term[i]);
        for j in range(1,window+1):
            len_2   =  len(index2term[i+j]);
            percent = similarity_normed(index2term[i],index2term[i+j],len_1,len_2,DIST);
            if percent > threshold:
                replacements.add((i+j,i,percent,));
    return replacements;

def replace_by_similar(index2term,threshold,window,DIST,compared):
    replacements = set([]);
    manager      = MP.Manager();
    tasks        = manager.Queue();
    results      = manager.Queue();
    T            = [];
    for i in range(len(index2term)-window):
        T += [(i+j,i,index2term[i],index2term[i+j],len(index2term[i]),len(index2term[i+j]),threshold,DIST,) for j in range(1,window+1) if not (index2term[i+j],index2term[i],) in compared];
        if len(T) > _batch2:
            compared |= set([(index2term[ij],index2term[i],) for ij,i,term_i,term_ij,len_1,len_2,threshold,DIST in T]);
            tasks.put(T);
            T = [];
    if len(T) != 0:
        tasks.put(T);
    workers = [MP.Process(target=get_similarity_normed,args=(tasks,results,x,)) for x in range(_jobs2)];
    for worker in workers:
        worker.start();
    for x in range(_jobs2):
        result = results.get();
        replacements |= result;
        #print 'Got result', x;
    for x in range(len(workers)):
        workers[x].join();
        #print 'Joined worker', x;
    return replacements, compared;

def get_similarity_normed(tasks,results,x):
    replacements = set([]);
    while True:
        print(x,'says: Approximate number of jobs in queue:', tasks.qsize());
        try:
            T = tasks.get(timeout=3);
            #print x,'says: Got', len(T), 'tasks to do...';
        except:
            break;
        for ij,i,term_i,term_ij,len_1,len_2,threshold,DIST in T:
            percent = similarity_normed(term_i,term_ij,len_1,len_2,DIST);
            if percent > threshold:
                replacements.add((ij,i,percent,));
        #print x,'says: Done with this set of tasks.';
    #print 'Closing job', x;
    results.put(replacements);
    return 0;

def prefix_normed(term1,term2,len_1,len_2):
    prefix  =  os.path.commonprefix([term1,term2]);
    is_prefix = len(prefix)==min([len_1,len_2]);
    return float(len(prefix))/max([len_1,len_2]) if is_prefix else 0.0;

def is_prefix(term1,term2):
    return term1 == term2[:len(term1)];

def make_affixes(terms):
    prefixed = {terms[0]:[(terms[0],0,)]};
    for i in range(len(terms)-1):
        affixes = [];
        pointer = 0;
        for prefix,interval in prefixed[terms[i]]:
            if len(prefix) >= 2 and interval < 10000 and is_prefix(prefix,terms[i+1][pointer:]): # reusing previous prefix
                affixes += [(prefix,interval+1,)];
                pointer += len(prefix);
            else:
                break;
        affixes             += [(terms[i+1][pointer:],0,)]; # new prefix
        prefixed[terms[i+1]] = affixes;
    return prefixed;

def similarity_normed(term1,term2,len_1,len_2,DIST):
    distance = damerau_dist(term1,term2) if DIST=='damerau' else edit_dist(term1,term2);
    return 1.-(float(distance)/max([len_1,len_2]));

def edit_dist(s1,s2):
    if len(s1) > len(s2):
        s1, s2 = s2, s1;
    distances = list(range(len(s1) + 1));
    for i2, c2 in enumerate(s2):
        distances_ = [i2+1];
        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1]);
            else:
                distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])));
        distances = distances_;
    return distances[-1];

def damerau_dist(s1,s2):
    oneago  = None;
    thisrow = list(range(1,len(s2)+1))+[0];
    for x in range(len(s1)):
        twoago, oneago, thisrow = oneago, thisrow, [0]*len(s2)+[x + 1];
        for y in range(len(s2)):
            delcost    = oneago[y] + 1;
            addcost    = thisrow[y-1] + 1;
            subcost    = oneago[y-1] + (s1[x]!=s2[y]);
            thisrow[y] = min(delcost,addcost,subcost);
            if (x>0 and y>0 and s1[x]==s2[y-1] and s1[x-1]==s2[y] and s1[x]!=s2[y]):
                thisrow[y] = min(thisrow[y],twoago[y-2]+1);
    return thisrow[len(s2)-1];

def all_partitions(seq):
    for cutpoints in range(1 << (len(seq)-1)):
        result = []
        lastcut = 0
        for i in range(len(seq)-1):
            if (1<<i) & cutpoints != 0:
                result.append(seq[lastcut:(i+1)])
                lastcut = i+1
        result.append(seq[lastcut:])
        yield result

def conflate(affixes,d,tree):
    max_score = 0;
    max_part  = None;
    for partitioning in all_partitions(affixes):
        summ = 0;
        prob = 1;
        for j in range(len(partitioning)):
            affix = ''.join(partitioning[j]);
            prob *= -np.log(tree[partitioning[j][-1]][1][partitioning[j+1][0]][2]) if j+1 < len(partitioning) else 1;#tree[partitioning[j][-1]][2];
            summ += d[(affix,)]**len(affix);
        score = summ / prob;
        if score > max_score:
            max_score = score;
            max_part  = [''.join(partition) for partition in partitioning];
    return max_part;

def replace_in_d(d,replace,threshold):
    denom = float(sum([d[key] for key in d]));
    d_    = Counter();
    for tup in d:
        d_[tuple([replace[term] if term in replace and d[term]/denom < threshold else term for term in tup])] += d[tup];
    return d_;

def add_in_d(d,transition):
    new_d    = dict();
    for tup in d:
        options = [els for els in itertools.product(*[[(term2,transition[term1][term2],) for term2 in transition[term1]] if term1 in transition else [(term1,1.,)] for term1 in tup])]
        for option in options:
            tup_, weights = zip(*option);
            new_d[tup_]   = functools.reduce(operator.mul,weights,1)*d[tup];
    d_ = {tup:new_d[tup] if tup in new_d else d[tup] for tup in set(d.keys())|set(new_d.keys())};
    return d_;

def build_counts(Q,R,x,):
    print('started', x);
    d, d_inv, dl, dl_inv = Counter(), Counter(), {lang:Counter() for lang in _langs}, {lang:Counter() for lang in _langs};
    while True:
        print(x,'says: Approximate number of jobs in queue:', Q.qsize());
        try:
            rows = Q.get(timeout=3);
        except:
            break;
        titles = [[term.strip().lower() for term in re.findall(WORD,row[0])] for row in rows if not row[0]==None];
        for title in titles:
            if len(title)==0: continue;
            titstr = ' '.join(title).strip();
            lang   = None;
            try:
                lang = detect(titstr);
            except:
                print(titstr);
            for n in range(1,_n_):
                for ngram in ngrams(title,n):
                    zgram                = tuple(reversed(ngram));
                    d[ngram]            += 1;
                    d_inv[zgram]        += 1;
                    dl[lang][ngram]     += 1;
                    dl_inv[lang][zgram] += 1;
        if len(d) > 100000:
            R.put((d,d_inv,dl,dl_inv,));
            d, d_inv, dl, dl_inv = Counter(), Counter(), {lang:Counter() for lang in _langs}, {lang:Counter() for lang in _langs};
    print('Closing job', x);
    R.put((d,d_inv,dl,dl_inv,));
    return 0;

def feed(Q,infolder):
    for inDB in os.listdir(infolder):
        print(infolder+inDB);
        con_title = sqlite3.connect(infolder+inDB);
        cur_title = con_title.cursor();
        cur_title.execute("SELECT c1 || ' ' || c2 || ' ' || c3 || ' ' || c4 FROM representations");
        i = 0;
        while True:
            i += 1; print(i*_batch);
            rows = cur_title.fetchmany(_batch);
            if len(rows) == 0:
                break;
            Q.put(rows);#Q.put(row[0]);
            while Q.qsize() > 1000:
                time.sleep(1);
        con_title.close();
    return 0;

def entropy(dist):
    return -sum([prob*math.log(prob,2) for prob in dist]);

#########################################################################################################################################
# SCRIPT ##########ä#####################################################################################################################

manager = MP.Manager();
Q       = manager.Queue();
R       = manager.Queue();

feeder  = MP.Process(target=feed,args=(Q,infolder,));
workers = [MP.Process(target=build_counts,args=(Q,R,x,)) for x in range(_jobs)];

feeder.start(); time.sleep(5);

for worker in workers:
    worker.start();

feeder.join();

for x in range(len(workers)):
    workers[x].join();
    print('Joined worker', x);

counts = [];
while not R.empty():
    result = R.get();
    counts.append(result);

print('Combining results');print(1);
d      = combine_counters((counts[x][0] for x in range(len(counts))));#sum((counts[x][0] for x in range(len(counts))),Counter());print(2);
d_inv  = combine_counters((counts[x][1] for x in range(len(counts))));
dl     = {lang: combine_counters((counts[x][2][lang] if lang in counts[x][2] else Counter() for x in range(len(counts)))) for lang in _langs}
dl_inv = {lang: combine_counters((counts[x][3][lang] if lang in counts[x][3] else Counter() for x in range(len(counts)))) for lang in _langs}
print('Done combining results.');
#input('Press Enter to continue...');

#########################################################################################################################################

_threshold_prefix  = 0.75#0.8; #TODO: These thresholds might be different for each typ
_window_prefix     = 1;
_threshold_similar = 0.8#.875;  #TODO: Try out what are the best ones for each type!
_window_similar    = 20;
_distance          = 'damerau'; #'edit'

terms         = sorted([gram[0] for gram in d if len(gram)==1]);
index2term    = copy(terms);
term2index    = {terms[i]:i for i in range(len(terms))};
term2index_   = copy(term2index);
replace       = dict();
sim_prefix    = dict();
sim_similar   = dict();
num_replace   = 99;
compared      = set();
while num_replace > 0:
    print(1);
    replace_prefix  = replace_by_prefix( terms,_threshold_prefix,_window_prefix);
    print(2);
    replace_similar, compared = replace_by_similar(terms,_threshold_similar,_window_similar,_distance,compared);
    #replace_edit    = replace_by_similar(terms,_threshold_similar,_window_similar,'edit');
    print(3);
    replace_new     = apply_replace(terms,replace_prefix|replace_similar);
    print(4);
    num_replace     = len(replace_new);
    print(5);
    only_prefix     = [(terms[pair[0]],terms[pair[1]],) for pair in replace_prefix-replace_similar];
    #only_damerau    = [(terms[pair[0]],terms[pair[1]],) for pair in replace_similar-replace_edit];
    print(6);
    replace.update(replace_new);
    print(7);
    terms       = sorted(list(set([replace[term] if term in replace else term for term in terms])));
    print(8);
    term2index_ = {terms[i]:i for i in range(len(terms))};
    print(num_replace, '(',len(only_prefix),len(compared),')');#, '(',len(only_damerau),')';

transition = term_transitions(replace);

_replace_thr = 1.;#0.00001; The frequency of the replaced item is in no way indicative of wether the replace makes sense or not.

d     = add_in_d(d    ,transition);#replace_in_d(d,    replace,_replace_thr);
d_inv = add_in_d(d_inv,transition);#replace_in_d(d_inv,replace,_replace_thr);

#########################################################################################################################################

tree     = make_tree(d);
tree_inv = make_tree(d_inv);

display(1,10,tree);
display(1,10,tree_inv,True);

#########################################################################################################################################

_and_p_ = 0.0005#0.5#0.6;
_and_c_ = 0.000000001#0.0001;
_or_p_  = 0.0003#0.3#0.4;
_or_c_  = 0.000000001#0.0001;
#-------------------------------------------------------------------------------------
#-right-min-left-min------------------------------------------------------------------
set_std   = set(probs_leq(_and_p_,_and_c_,tree));
set_inv   = set([tuple(reversed(el)) for el in probs_leq(_and_p_,_and_c_,tree_inv)]);
inter     = set_std & set_inv;
#-------------------------------------------------------------------------------------
#-right-certain-left-min--------------------------------------------------------------
set_std_  = set(probs_leq(1.0,_or_c_,tree));
set_inv_  = set([tuple(reversed(el)) for el in probs_leq(_or_p_,_or_c_,tree_inv)]);
inter_    = set_std_ & set_inv_;
#-left-certain-right-min--------------------------------------------------------------
#-------------------------------------------------------------------------------------
set_std__ = set(probs_leq(_or_p_,_or_c_,tree));
set_inv__ = set([tuple(reversed(el)) for el in probs_leq(1.0,_or_c_,tree_inv)]);
inter__   = set_std__ & set_inv__;
#-------------------------------------------------------------------------------------
union     = inter | inter_ | inter__;
#-------------------------------------------------------------------------------------
print(union);
print(len(inter), '+', len(inter_), '->', len(inter|inter_), '+', len(inter__), '->', len(union));

OUT = open(phrfile,'w');
for tup in union:
    OUT.write(tup[0]+' '+tup[1]+'\n');
OUT.close();

OUT = open(trafile,'w');
for key in replace:
    OUT.write(key+' '+replace[key]+'\n');
OUT.close();

#OUT = open(modfile,'w');
#for term1 in transition:
#    for term2 in transition[term1]:
#        OUT.write(term1+' '+term2+' '+str(transition[term1][term2])+'\n');
#OUT.close();

#########################################################################################################################################

terms     = terms;
terms_inv = sorted([term[::-1] for term in terms]);

affixes_of  = make_affixes(terms);
suffixes_of = make_affixes(terms_inv);
suffixes_of = {key[::-1]:[(affix[::-1],num,) for affix,num in suffixes_of[key][::-1]] for key in suffixes_of}

agrams   = ([(affixes_of[term][i][0],affixes_of[term][i+1][0],) for i in range(len(affixes_of[term])-1)]+[(affixes_of[term][-1][0],)] if len(affixes_of[term])>=2 else [(affixes_of[term][-1][0],)] for term in affixes_of);
agrams   = [gram for grams in agrams for gram in grams];
zgrams   = ([(affixes_of[term][i+1][0],affixes_of[term][i][0],) for i in range(len(affixes_of[term])-1)]+[(affixes_of[term][0][0],)] if len(affixes_of[term])>=2 else [(affixes_of[term][0][0],)] for term in affixes_of);
zgrams   = [gram for grams in zgrams for gram in grams];
tree     = make_tree(Counter(agrams));
tree_inv = make_tree(Counter(zgrams));

affixes_of_ = dict();
count = 0;
for term in affixes_of:
    count += 1;
    if count == 1000000:
        pass;#break;
    affixes_of_[term] = [];
    current           = affixes_of[term][0];
    for i in range(len(affixes_of[term])):
        if i == len(affixes_of[term])-1:
             affixes_of_[term] += [current];
        elif not (affixes_of[term][i],affixes_of[term][i+1],) in inter_:
            current += affixes_of[term][i+1];
        else:
            affixes_of_[term] += [current];
            current            = affixes_of[term][i+1];
    print(affixes_of_[term]);

agram_counter = Counter();
for term in affixes_of:
    affixes = [affix for affix,dist in affixes_of[term]];
    for i in range(len(affixes)):
        ngram = tuple(affixes[:i+1]);#+[None for j in range(i+1,len(affixes))]);
        agram_counter[ngram] = d[(''.join([gram for gram in ngram if gram!=None]),)];
        print(ngram,d[(term,)]);#d[(''.join([gram for gram in ngram if gram!=None]),)]);
tree = make_tree_(agram_counter);

splits = dict();
get_splits([],tree[1],splits,True,0.01,3);

leftovers = set();
for key in splits:
    LIST  = [''] + splits[key];
    PARTS = [LIST[i][len(LIST[i-1]):] for i in range(len(LIST))][1:];
    leftovers.add(PARTS[-1]);

terms_inv   = sorted([term[::-1] for term in leftovers]);
suffixes_of = make_affixes(terms_inv);
suffixes_of = {key[::-1]:[(affix[::-1],num,) for affix,num in suffixes_of[key][::-1]] for key in suffixes_of}

zgram_counter = Counter();
for term in suffixes_of:
    suffixes = [affix for affix,dist in suffixes_of[term]];
    for i in range(len(suffixes)):
        ngram = tuple(suffixes[:i+1]);#+[None for j in range(i+1,len(affixes))]);
        key = ''.join([gram for gram in ngram if gram!=None]);
        zgram_counter[ngram] = d[(key,)] if (key,) in d else 0.000000001;
        #print(ngram,d[(term,)]);#d[(''.join([gram for gram in ngram if gram!=None]),)]);
tree_inv = make_tree_(zgram_counter);

splits_inv = dict();
get_splits([],tree_inv[1],splits_inv,True,0.01,3);

splittings = dict();
for key in splits:
    LIST   = [''] + splits[key];
    PARTS  = [LIST[i][len(LIST[i-1]):] for i in range(len(LIST))][1:];
    ENDS   = [''] + splits_inv[PARTS[-1]] if PARTS[-1] in splits_inv else ['',PARTS[-1]];
    PARTS_ = [ENDS[i][len(ENDS[i-1]):] for i in range(len(ENDS))][1:];
    SPLITS = PARTS[:-1]+PARTS_;
    print('--------------------');
    additionals = [PART for PART in PARTS_  if (PART,) in d and len(PART)>2 and d[(PART,)]>50];
    additionals_ = [PART for PART in [''.join(ngram) for ngram in ngrams(PARTS_,2)+ngrams(PARTS,3)+ngrams(PARTS,4)]  if (PART,) in d and len(PART)>2 and d[(PART,)]>0];
    prefixes    = [''.join(PARTS [:i]) for i in range(1,len(PARTS )+1)];
    suffixes    = [''.join(SPLITS[:i]) for i in range(1,len(SPLITS)+1)];
    print(set(prefixes+additionals+additionals_));
    splittings[key] = set(prefixes+additionals+additionals_);

OUT = open(modfile,'w');
for term in splittings:
    OUT.write(term+' '+' '.join((split for split in splittings[term] if split != term))+'\n');
OUT.close();

'''
for root in tree[1]:
    print('---------------------------------------\n'+root+'\n---------------------------------------');
    print(tree[1][root]);
    print('---------------------------------------\n');

lines_ = [];
for line in lines:
    if len(line)<=1:
        lines_.append(line);
        continue;
    else:
        line_ = [];
    bigrams = ngrams(line,2);
    phrase  = bigrams[0][0];
    for bigram in bigrams:
        if bigram in union:
            phrase += '_'+bigram[1];
        else:
            line_ += [phrase];
            phrase = bigram[1];
    if len(line_)==0 or not line_[-1].endswith(phrase): line_.append(phrase);
    lines_.append(line_);

reps = [];
for i in range(len(lines_)):
    rep = set([string for string in lines_[i] if len(string)>=3]) - illeg;
    reps.append(rep);

for i in range(len(reps)):
    if len(reps[i])>_max_len_:
        rep = set([tup[1] for tup in sorted([(d[el],el) for el in rep])][:_max_len_]);
'''
