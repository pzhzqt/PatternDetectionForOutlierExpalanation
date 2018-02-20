import pandas as pd
from itertools import combinations
import statsmodels.formula.api as sm
from scipy.stats import chisquare,mode
from numpy import percentile,mean
from time import time
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
    
    def __init__(self, conn, theta_c=0.75, theta_l=0.75, lamb=0.8):
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
#         self.fd={}
        for col in self.schema:
#             if col=='year':
#                 self.num.append(col)
#             elif col!='id':
#                 self.cat.append(col)
                
            
            try:
                float(self.schema[col])
                self.num.append(col)
            except:
                self.cat.append(col)
                
    
    def findPattern(self):
#        self.pc=PC.PatternCollection(list(self.schema))
        self.createTable()
        start=time()
        
        for a in self.schema:
            if a in self.num:
                aggList=["count","sum"]
            else:
                aggList=["count"]
            for agg in aggList:                
                col_all=[col for col in self.cat+self.num if col!=a]
                if len(col_all)>4:
                    col_4=combinations(col_all,4)
                else:
                    col_4=[col_all]
                for cols in col_4:
                    #self.formCube(a,agg,cols)
                    for i in range(min(len(cols),4),0,-1):
                        for g in combinations(cols,i):
                            #d=pd.read_sql(self.aggQuery(g,cols),con=self.conn)
                            for j in range(len(g)-1,0,-1):
                            #q: Do we allow f to be empty set?
                                for v in combinations(g,j):
                                    v=list(v)
                                    f=[k for k in g if k not in v]
                                    l=0 #l indicates if we fit linear model
                                    if all([x in self.num for x in v]):
                                        l=1
                                    self.fitmodel(cols,f,v,a,agg,l)
                    #self.dropCube()
        
        end=time()
        self.conn.execute('INSERT INTO time(time) values('+str(end-start)+');')
    def formCube(self, a, agg, attr):
        group=",".join(["CAST("+num+" AS NUMERIC)" for num in self.num if num!=a and num in attr]+
                        [cat for cat in self.cat if cat!=a and cat in attr])
        grouping=",".join(["CAST("+num+" AS NUMERIC), GROUPING(CAST("+num+" AS NUMERIC)) as g_"+num
                        for num in self.num if num!=a and num in attr]+
            [cat+", GROUPING("+cat+") as g_"+cat for cat in self.cat if cat!=a and cat in attr])
        if a in self.num:
            a="CAST("+a+" AS NUMERIC)"
        query="CREATE TABLE cube AS SELECT "+agg+"("+a+"), "+grouping+" FROM "+self.table+" GROUP BY CUBE("+group+")"
        self.conn.execute(query)
        indx=",".join(["g_"+col for col in attr])
        self.conn.execute("CREATE INDEX in_a on cube("+indx+");")
    
    def dropCube(self):
        self.conn.execute("DROP TABLE cube;")
        
    def aggQuery(self, g, a, agg, f):
        #=======================================================================
        # res=" and ".join([a+".notna()" for a in g])
        # if len(g)<len(cols):
        #     null=" and ".join([b+".isna()" for b in cols if b not in g])
        #     res=res+" and "+null
        #=======================================================================
        #res=" and ".join(["g_"+a+"=0" for a in g])
        #if len(g)<len(cols):
        #    unused=" and ".join(["g_"+b+"=1" for b in cols if b not in g])
        #    res=res+" and "+unused
        group=",".join(["CAST("+num+" AS NUMERIC)" for num in self.num if num!=a and num in g]+
                        [cat for cat in self.cat if cat!=a and cat in g])
        return "SELECT "+group+","+agg+"("+a+")"+" FROM "+self.table+" GROUP BY "+group+" ORDER BY "+",".join(f)
    
    def fitmodel(self,cols,f,v,a,agg,l=0):
        #fd=d.sort_values(by=f).reset_index(drop=True)
        fd=pd.read_sql(self.aggQuery(f+v,a,agg,f),con=self.conn)
        oldKey=None
        oldIndex=0
        num_f=0
        valid_l_f=0
        valid_c_f=0
        
        for tup in fd.itertuples():
            thisKey=tup
            if oldKey and any(map(lambda attr:getattr(thisKey,attr)!=getattr(oldKey,attr),f)):
                index=tup.Index
                temp=fd[oldIndex:index]
                num_f+=1
                if len(temp[agg])>=10:
                    describe=[mean(temp[agg]),mode(temp[agg]),percentile(temp[agg],25)
                                      ,percentile(temp[agg],50),percentile(temp[agg],75)]
                    if l==1: #fitting linear
                        lr=sm.ols(agg+'~'+'+'.join(v),data=temp).fit()
                        theta_l=lr.rsquared_adj
                        if theta_l and theta_l>self.theta_l:
                            valid_l_f+=1
                        #self.pc.add_local(f,oldKey,v,a,agg,'linear',theta_l)
                            self.conn.execute(self.addLocal(f,oldKey,v,a,agg,'linear',theta_l,describe))
                            
                    #fitting constant
                    theta_c=chisquare(temp[agg])[1]
                    if theta_c>self.theta_c:
                        valid_c_f+=1
                        #self.pc.add_local(f,oldKey,v,a,agg,'const',theta_c)
                        self.conn.execute(self.addLocal(f,oldKey,v,a,agg,'const',theta_c,describe))
                        
                oldIndex=index
            oldKey=thisKey
            
        if oldKey:
            temp=fd[oldIndex:]
            num_f+=1
            if len(temp[agg])>=10:
                describe=[mean(temp[agg]),mode(temp[agg]),percentile(temp[agg],25,interpolation='nearest')
                                      ,percentile(temp[agg],50,interpolation='nearest'),
                                      percentile(temp[agg],75,interpolation='nearest')]
                if l==1:
                    lr=sm.ols(agg+'~'+'+'.join(v),data=temp).fit()
                    theta_l=lr.rsquared_adj
                    if theta_l>self.theta_l:
                        valid_l_f+=1
                        #self.pc.add_local(f,oldKey,v,a,agg,'linear',theta_l)
                        self.conn.execute(self.addLocal(f,oldKey,v,a,agg,'linear',theta_l,describe))
                theta_c=chisquare(temp[agg])[1]
                if theta_c>self.theta_c:
                    valid_c_f+=1
                    #self.pc.add_local(f,oldKey,v,a,agg,'const',theta_c)
                    self.conn.execute(self.addLocal(f,oldKey,v,a,agg,'const',theta_c,describe))
        
        lamb_c=valid_c_f/num_f
        lamb_l=valid_l_f/num_f
        if lamb_c>self.lamb:
            #self.pc.add_global(f,v,a,agg,'const',self.theta_c,lamb_c)
            self.conn.execute(self.addGlobal(f,v,a,agg,'const',self.theta_c,lamb_c))
        if lamb_l>self.lamb:
            #self.pc.add_global(f,v,a,agg,'linear',str(self.theta_l),str(lamb_l))
            self.conn.execute(self.addGlobal(f,v,a,agg,'linear',self.theta_l,lamb_l))
            
    def addLocal(self,f,f_val,v,a,agg,model,theta,describe):
        f="'"+str(f).replace("'","")+"'"
        f_val="'"+str(f_val).replace("'","")+"'"
        v="'"+str(v).replace("'","")+"'"
        a="'"+a+"'"
        agg="'"+agg+"'"
        model="'"+model+"'"
        theta="'"+str(theta)+"'"
        describe="'"+str(describe).replace("'","")+"'"
        return 'insert into '+self.table+'_local values('+','.join([f,f_val,v,a,agg,model,theta,describe])+');'
    
    def addGlobal(self,f,v,a,agg,model,theta,lamb):
        f="'"+str(f).replace("'","")+"'"
        v="'"+str(v).replace("'","")+"'"
        a="'"+a+"'"
        agg="'"+agg+"'"
        model="'"+model+"'"
        theta="'"+str(theta)+"'"
        lamb="'"+str(lamb)+"'"
        return 'insert into '+self.table+'_global values('+','.join([f,v,a,agg,model,theta,lamb])+');'
    
    def createTable(self):
        self.conn.execute('create table IF NOT EXISTS '+self.table+'_local('+
                     'fixed varchar,'+
                     'fixed_value varchar,'+
                     'variable varchar,'+
                     'in_a varchar,'+
                     'agg varchar,'+
                     'model varchar,'+
                     'theta float,'+
                     'stats varchar);')
        self.conn.execute('DELETE FROM '+self.table+'_local;')
        
        self.conn.execute('create table IF NOT EXISTS '+self.table+'_global('+
                     'fixed varchar,'+
                     'variable varchar,'+
                     'in_a varchar,'+
                     'agg varchar,'+
                     'model varchar,'+
                     'theta float,'+
                     'lambda float);')
        self.conn.execute('DELETE FROM '+self.table+'_global')
        
        self.conn.execute('create table IF NOT EXISTS time('+
                          'time varchar,'+
                          'description varchar);')
        
