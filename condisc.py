import csv
from itertools import combinations


class GlobalRegressionConstraint(object):
    """      
        Attributes:
            F: set of indices of fixed attributes
            V: set of indices of variable attributes
            B: index of attribute to be aggregated
            agg: aggregate function
            model: regression model type of agg(B) on V
            _theta: threshold of goodness of fit
            _lambda: percentage of f that satisfies the model
    """

    def __init__(self, f, v, b, agg, model, _theta, _lambda):
        self.F = f
        self.V = v
        self.B = b
        self.agg = agg
        self.model = model
        self._theta = _theta
        self._lambda = _lambda


class SetTrie(object):

    def __init__(self):
        self.root = TrieEntry()

    def add_group(self, g, l):
        """
            type g: tuple of int
            type l: list of constraints
        """
        cur = self.root
        for i in range(len(g)):
            if g[i] not in cur.children:
                cur.make_child(g[i])
            cur = cur.children[g[i]]
        cur.cons = l

    def search_subset(self, g):
        """
        :param g: tuple of int
        :return: list of constraints
        """
        global res
        res = []
        self.__search_subset(self.root, g, 0)
        return res

    def __search_subset(self, te, g, start):
        for i in range(start,len(g)):
            if g[i] in te.children:
                ne = te.children[g[i]]
                if ne.cons:
                    res.extend(ne.cons)
                self.__search_subset(ne, g, i+1)


class TrieEntry(object):
    """
        Attributes:
            val: int that represents the new attribute in this group
            children: dictionary that points to the group that has G
                      as prefix
            cons: pointer to the list of constraints under the group
    """
    def __init__(self, val=None):
        self.val = val
        self.children = {}
        self.cons = None

    def make_child(self, att):
        """
            type att: int
        """
        self.children[att] = TrieEntry(att)


class GlobalConstraintCollection(object):
    """
        Attributes:
            header: list of attribute names
            index: dict that maps attribute names to their index in header
            B: list of dictionaries. Each index is a B, and each dictionary
               maps an aggregate function agg to a dictionary that maps a
               group G to a list of constraints with that G,B, and agg
            cur_set: list of dictionaries. Each index is a B, and each
                     dictionary maps an aggregate function to the root of the
                     trie that stores the info of existing groups
    """
    
    def __init__(self,header):
        """
        type header: list of strings
        """
        self.header = header
        for i in range(len(header)):
            self.index[header[i]] = i
        self.B = [{} for i in range(len(header))]
        self.cur_set = [{} for i in range(len(header))]
        
    def search(self, b, g, agg):
        """
        type b: string
        type g: set of strings
        type agg: string
        """
        return self.__search_int(self.index[b],
                                 tuple([self.index[s] for s in g].sort()), agg)

    def __search_int(self, b, g, agg):
        if g in self.B[b][agg]:
            return self.B[b][agg][g]
        else:
            return None

    def search_subset(self, b, g, agg):
        """
        type b: string
        type g: set of strings
        """
        b = self.index[b]
        g = tuple([self.index[s] for s in g].sort())
        if agg not in self.cur_set[b]:
            return None
        else:
            return self.cur_set[b][agg].search_subset(g)

    """
    def __search_powerset(self, b, g, agg):
        res = []
        for k in range(1,len(g)+1):
            for i in combinations(g, k):
                c = self.__search_int(b, g, agg)
                if c:
                    res.append(c)
        return res
    """
    def add_constraint(self, f, v, b, agg, model, _theta, _lambda):
        """      
            type F: set of strings
            type V: set of strings
            type B: string
            type agg: string
            model: string
            _theta:
            _lambda:
        """
        f = set([self.index[s] for s in f])
        v = set([self.index[s] for s in v])
        b = self.index[b]
        grc = GlobalRegressionConstraint(f, v, b, agg, model, _theta, _lambda)
        g = tuple(f.union(v))
        try:
            self.B[b][agg][g].append(grc)
        except:
            self.B[b][agg][g] = [grc]
            if agg not in self.cur_set[b]:
                self.cur_set[b][agg] = SetTrie()
            self.cur_set[b][agg].add_group(g, self.B[b][agg][g])
                

def build_global_constraint(datafile):
    """
    only consider count for now

    for each attribute B in data:
        for each proper, non-empty subset F of {all_attributes-B}:
            for each non-empty subset V of {all_attributes-B-F}:
                for each aggregate function agg in func:
                    temp=select agg(B) from data group by F,V
(question: should we predefine theta or lambda?)
                    for each type in {set of model type}:
                        count=0
                        for each f in F:
                            model=train(type,V->agg(B))
                            if goodness_of_fit(model)>theta:
                                count+=1
                        GRC=GlobalRegressionConstraint(F,V,B,type,agg,theta,count/|F|)
                        GlobalConstraintCollection.add(GRC)
                            
                        

    """
    file=open(datafile)
    data=csv.reader(file,delimiter=",")
    header=next(data)
    n=len(header)
    a_a=set([i for i in range(n)])
    for B in range(n):
        for k in range(n-1,1,-1):
            G_set=[set(i) for i in combinations(a_a-{B},k)]
            for G in G_set:
                file.seek(0)
                next(data)
                agg_result={}
                for row in data:
                    try:
                        agg_result[tuple(row[i] for i in G)]+=1
                    except:
                        agg_result[tuple(row[i] for i in G)]=1
                for F_size in range(1,k):
                    F_set=[set(i) for i in combinations(G,F_size)]
                    for F in F_set:
                        V=G-F
                        
