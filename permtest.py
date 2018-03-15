from itertools import combinations,permutations

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

#when there are n numbers and group, return the next available for group[-1]
def nextnum(group,n):
    res=(group[-1]+1)%n
    if len(group)>1:
        while res in group[:-1]:
            res=(res+1)%n
    return res

#find the prefix that we want to do regression for this group. If a prefix
#will be taken care of by a different grouping, then not do it.            
def findpre(group,d_index,n):
    for k in range(d_index,0,-1):
        if group[k]!=nextnum(group[:k],n):
            return k
    return 0

def main():
    n=8
    l=[i for i in range(n)]
    
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
                pre=findpre(perm,len(perm)-1,n)
                add_rollup(perm_group,perm,pre)
            else:
                pre=findpre(perm,d_index,n)
                if pre==d_index:
                    continue
                else:
                    add_rollup(perm_group,perm,pre,d_index)
    	    
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

if __name__=="__main__":
    main()
