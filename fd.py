def closure(fd,n,a):
    '''
    type fd: list of tuple where each tuple = (lhs,rhs) where lhs,rhs are lists of int
    type a:tuple of int
    '''
    m=len(fd)
    c=[0]*m
    lhs=[[] for i in range(n)]
    rhs=[[] for i in range(m)]
    aplus=set()
    todo=list(a)
    
    for i in range(m):
        left=fd[i][0]
        right=fd[i][1]
        c[i]=len(left)
        for attr in left:
            lhs[attr].append(i)
        for attr in right:
            rhs[i].append(attr)
    
    while todo:
        curA=todo.pop()
        aplus.add(curA)
        for fd in lhs[curA]:
            c[fd]-=1
            if c[fd]==0:
                for newA in rhs[fd]:
                    if newA not in aplus:
                        todo.append(newA)
                        
    return aplus

def allClosure(fd,attrs):
    '''
    type fd: list of tuple where each tuple = (lhs,rhs) where lhs,rhs are lists of strings
    type attrs: list of strings
    '''
    n=len(attrs)
    index={}
    for i in range(len(attrs)):
        index[attrs[i]]=i
        
    for tup in fd:
        for i in range(2):
            for j in range(len(tup[i])):
                try:
                    tup[i][j]=index[tup[i][j]]
                except KeyError as ex:
                    print(str(ex)+" is not in the table")
                    return {}
                
    A=set()
    for tup in fd:
        a=tuple(sorted(tup[0]))
        if a not in A:
            A.add(a)
    
    fdplus={}
    for a in A:
        fdplus[a]=closure(fd,n,a)
    return fdplus

def main():
    attrs=['A','B','C','G','H','I','J','K']
    fd=[(['A'],['B']),(['A'],['C']),(['C','G'],['H']),(['C','G'],['I']),(['B'],['H'])]
    a=['A','G']
    print(allClosure(fd,attrs))
    
if __name__=="__main__":
    main()
    
