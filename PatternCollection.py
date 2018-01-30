import csv
from itertools import combinations


class GlobalRegressionConstraint(object):
    """      
        Attributes:
            F: set of indices of fixed attributes
            V: set of indices of variable attributes
            a: index of attribute to be aggregated
            agg: aggregate function
            model: regression model type of agg(B) on V
            _theta: threshold of goodness of fit
            _lambda: percentage of f that satisfies the model
    """

    def __init__(self, f, v, a, agg, model, _theta, _lambda):
        self.F = f
        self.V = v
        self.a = a
        self.agg = agg
        self.model = model
        self._theta = _theta
        self._lambda = _lambda


class SetTrie(object):

    def __init__(self):
        self.root = TrieEntry()

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
            if g[i] in te.child:
                ne = te.child[g[i]]
                if ne.val:
                    res.extend(ne.val)
                self.__search_subset(ne, g, i+1)


class TrieEntry(object):
    """
        Attributes:
            child: dictionary that points to the group that has G
                      as prefix
            val: array of the list of constraints under the group
    """
    def __init__(self):
        self.val = None
        self.child = {}


class GlobalConstraintCollection(object):
    """
        Attributes:
            header: list of attribute names
            index: dict that maps attribute names to their index in header
            a: list of dictionaries. Each index is a B, and each
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
        self.a = [{} for i in range(len(header))]
        
    def search(self, a, g, agg):
        """
        type a: string
        type g: set of strings
        type agg: string
        """
        return self.__search_int(self.index[a],
                                 tuple([self.index[s] for s in g].sort()), agg)

    def __search_int(self, a, g, agg):
        if g in self.a[a][agg]:
            return self.a[a][agg][g]
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

    