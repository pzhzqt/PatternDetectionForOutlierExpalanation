import pandas as pd
from itertools import combinations
from sklearn.linear_model import LinearRegression
import PatternCollection as PC

class PatternFinder:
    conn=None
    table=None
    theta_c=None
    theta_l=None
    lamb=None
    cat=None
    num=None
    schema=None
    pc=None
    
    def __init__(self, conn, theta_c=0.15, theta_l=0.75, lamb=0.8):
        self.conn=conn
        self.theta_c=theta_c
        self.theta_l=theta_l
        self.lamb=lamb
        
        while(True):
            try:
                self.table=input("Relation name: \n")
                self.schema=pd.read_sql("SELECT * FROM "+self.table+" LIMIT 1",self.conn)
                break
            except Exception as ex:
                print(ex)
        
        self.cat=[]
        self.num=[]
        for col in self.schema:
            try:
                float(self.schema[col])
                self.num.append(col)
            except ValueError:
                self.cat.append(col)
    
    def findPattern(self):
        pc=PC.PatternCollection(list(self.schema))
        
        for a in self.cat:
            agg="count"
            cube=pd.read_sql(self.formCube(a,agg), self.conn)
            cols=[col for col in self.schema if col!=a]
            for i in range(len(cols),0,-1):
                for g in combinations(cols,i):
                    d=cube.query(self.aggQuery(g,cols))
                    for j in range(len(g)-1,0,-1):
                    #q: Do we allow f to be empty set?
                        for v in combinations(g,j):
                            v=list(v)
                            f=[k for k in g if k not in v]
                            l=0 #l indicates if we fit linear model
                            if all([x in self.num for x in v]):
                                l=1
                            self.fitmodel(d,f,v,a,agg,l)
    
    def formCube(self, a, agg):
        group=",".join(["CAST("+num+" AS NUMERIC)" for num in self.num if num!=a]+
            [cat for cat in self.cat if cat!=a])
        query="SELECT "+agg+"("+a+"), "+group+" FROM "+self.table+" GROUP BY CUBE("+group+")"
        return query
        
    def aggQuery(self, g, cols):
        res=" and ".join([a+".notna()" for a in g])
        if len(g)<len(cols):
            null=" and ".join([b+".isna()" for b in cols if b not in g])
            res=res+" and "+null
        return res
    
    def fitmodel(self,d,f,v,a,agg,l=0):
        fd=d.sort_values(by=f)
        oldKey=None
        oldIndex=0
        num_f=0
        valid_l_f=0
        valid_c_f=0
        
        for index,row in fd.iterrows():
            thisKey=row[f]
            if oldKey is not None and any(oldKey!=thisKey):
                temp=fd[oldIndex:index]
                num_f+=1
                
                if l==1: #fitting linear
                    lr=LinearRegression()
                    lr.fit(temp[v],temp[agg])
                    theta_l=lr.score(temp[v],temp[agg])
                    if theta_l>self.theta_l:
                        valid_l_f+=1
                        pc.add_local(f,list(oldKey),v,a,agg,'linear',theta_l)
                        print('adding local: '+str(f)+','+str(list(oldKey))+','+str(v)+','+
                              str(a)+','+agg+','+'linear'+','+theta_l)
                        
                #fitting constant
                theta_c=pd.DataFrame.std(temp[agg])/pd.DataFrame.mean(temp[agg])
                if theta_c<self.theta_c:
                    valid_c_f+=1
                    pc.add_local(f,list(oldKey),v,a,agg,'const',theta_c)
                    print('adding local: '+str(f)+','+str(list(oldKey))+','+str(v)+','+
                              str(a)+','+agg+','+'const'+','+theta_c)
                    
                oldIndex=index
            oldKey=thisKey
            
        if oldKey is not None:
            temp=fd[oldIndex:]
            num_f+=1
            
            if l==1:
                lr=LinearRegression()
                lr.fit(temp[v],temp[agg])
                theta_l=lr.score(temp[v],temp[agg])
                if theta_l>self.theta_l:
                    valid_l_f+=1
                    pc.add_local(f,list(oldKey),v,a,agg,'linear',theta_l)
                    print('adding local: '+str(f)+','+str(list(oldKey))+','+str(v)+','+
                              str(a)+','+agg+','+'linear'+','+theta_l)
            theta_c=pd.DataFrame.std(temp[agg])/pd.DataFrame.mean(temp[agg])
            if theta_c<self.theta_c:
                valid_c_f+=1
                pc.add_local(f,list(oldKey),v,a,agg,'const',theta_c)
                print('adding local: '+str(f)+','+str(list(oldKey))+','+str(v)+','+
                              str(a)+','+agg+','+'const'+','+theta_c)
        
        lamb_c=valid_c_f/num_f
        lamb_l=valid_l_f/num_f
        if lamb_c>self.lamb:
            pc.add_global(f,v,a,agg,'const',self.theta_c,lamb_c)
        if lamb_l>self.lamb:
            pc.add_global(f,v,a,agg,'linear',self.theta_l,lamb_l)
        