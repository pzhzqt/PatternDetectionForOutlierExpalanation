from itertools import combinations,permutations

class SetTrie():
    def __init__(self):
        self.root=None

    #add attrs as the existing prefix to the Trie
    def add(self,attrs):
        if not self.root:
            self.root=TrieEntry()
        cur=self.root
        for attr in attrs:
            if attr not in cur.child:
                cur.child[attr]=TrieEntry()
            cur=cur.child[attr]
        return

    #search the existing prefix of tuple attrs, return the length of prefix
    def search(self,attrs):
        if not self.root:
            return 0
        cur=self.root
        n=len(attrs)
        for i in range(n):
            if attrs[i] not in cur.child:
                return i
            cur=cur.child[attrs[i]]
        return n

class TrieEntry():
    def __init__(self):
        self.child={}

def add_rollup(dic,group,prefix,d_index=None):
    if d_index:
        n=d_index
    else:
        n=len(group)
    for k in range(n,prefix,-1):
        for i in range(k):
            key=tuple(sorted(group[:k]))
            if key not in dic:
                dic[key]=set()
            dic[key].add(tuple(sorted(group[k-i-1:k])))

#check if a prefix will be taken care of by another grouping
#input prefix will for sure have one and only one decrease
#return True if it won't
def check_format(pre,n):
    k=len(pre)
    cnt=0
    cur=pre[-1]
    for i in range(k-1):
        if pre[i]>cur:
            cnt+=1
    return n-cur-cnt<4-k #it will be taken care of by another grouping

n=6
l=[i for i in range(1,n+1)]

def increasing(l):
    for i in range(len(l)-1):
        if l[i]>=l[i+1]:
            return False
    return True

all_group={}
for i in range(4,0,-1):
    groups=combinations(l,i)
    for group in groups:
        all_group[group]=set()
        for j in range(len(group),0,-1):
            vs=combinations(group,j)
            for v in vs:
                all_group[group].add(v)

perm_group={}
prefix=SetTrie()
combs=combinations(l,min(4,len(l)))
for comb in combs:
    perms=permutations(comb,len(comb))
    for perm in perms:
        decrease=0
        d_index=None
        for i in range(1,len(perm)):
            if perm[i-1]>perm[i]:
                decrease+=1
            if decrease==2:
                d_index=i #perm[:d_index] will decrease at most once

        if not d_index:
            pre=prefix.search(perm)
            add_rollup(perm_group,perm,pre)
        else:
            pre=prefix.search(perm[:d_index])
            if pre==d_index:
                continue
            else:
                if check_format(perm[:d_index],n):
                    add_rollup(perm_group,perm,pre,d_index)
        
##    decrease=0
##    for i in range(1,len(group)):
##        if group[i]<group[i-1]:
##            decrease+=1
##    if decrease>=2:
##        continue
##    for j in range(len(group),0,-1):
##        key=tuple(sorted(group[:j]))
##        if key not in perm_group:
##            perm_group[key]=set()
##        for k in range(j,0,-1):
##            v=group[:j][-k:]
##            if not increasing(v):
##                continue
##            if not increasing(group[:j][:-k]):
##                continue
##            perm_group[key].add(v)

dif={}
for key in all_group:
    if key not in perm_group:
        dif[key]=all_group[key]
    else:
        for v in all_group[key]:
            if v not in perm_group[key]:
                if key not in dif:
                    dif[key]=set()
                dif[key].add(v)

print(dif)
