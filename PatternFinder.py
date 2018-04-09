import pandas as pd
from itertools import combinations
#import statsmodels.formula.api as sm
from sklearn.linear_model import LinearRegression
from scipy.stats import chisquare,mode
from numpy import percentile,mean
from time import time
from permtest import *

class PatternFinder:
    conn=None
    table=None
    theta_c=None
    theta_l=None
    lamb=None
    cat=None
    num=None
    schema=None
    grouping_attr=None
    c_star=False
    pc=None
    fit=None
    dist_thre=None
    
    def __init__(self, conn, table, fit=True, theta_c=0.75, theta_l=0.75, lamb=0.8, dist_thre=0.9):
        self.conn=conn
        self.theta_c=theta_c
        self.theta_l=theta_l
        self.lamb=lamb
        self.fit=fit
        self.dist_thre=dist_thre
        
        try:
            self.table=table
            self.schema=list(pd.read_sql("SELECT * FROM "+self.table+" LIMIT 1",self.conn))
        except Exception as ex:
            print(ex)
        
        self.cat=[]
        self.num=[]
        self.grouping_attr=[]
#         self.fd={}
        #check uniqueness, grouping_attr contains only non-unique attributes
        unique=pd.read_sql("SELECT attname,n_distinct FROM pg_stats WHERE tablename="+table)
        for tup in unique.itertuples():
            if tup.n_distinct > -self.dist_thre:
                self.grouping_attr.append(tup.attname)
                
        for col in self.schema:
#             if col=='year':
#                 self.num.append(col)
#             elif col!='id':
#                 self.cat.append(col)
            try:
                self.conn.execute("SELECT CAST("+col+" AS NUMERIC) FROM "+self.table)
                self.num.append(col)
            except:
                self.cat.append(col)
                
    
    def findPattern(self):
#       self.pc=PC.PatternCollection(list(self.schema))
        self.createTable()
        start=time()
        
        for a in self.schema+['*']:
            if a=='*':
                aggList=["count"]
            elif a in self.grouping_attr:
                if a in self.num:
                    aggList=["count","sum"]
                else:
                    aggList=["count"]
            else:
                if a in self.num:
                    aggList=["sum"]
                else:# for all unique in_a, ignore
                    continue
            for agg in aggList:
                cols=[col for col in self.grouping_attr if col!=a]
                n=len(cols)
                combs=combinations([i for i in range(n)],min(4,n))
                for comb in combs:
                    grouping=[cols[i] for i in comb]
                    self.aggQuery(grouping,a,agg)
                    perms=permutations(comb,len(comb))
                    for perm in perms:
                        decrease=0
                        d_index=None
                        division=None
                        for i in range(1,len(perm)):
                            if perm[i-1]>perm[i]:
                                decrease+=1
                                if decrease==1:
                                    division=i #f=group[:divition],v=group[division:] is the only division
                                elif decrease==2:
                                    d_index=i #perm[:d_index] will decrease at most once
                                    break
            
                        if not d_index:
                            d_index=len(perm)
                            pre=findpre(perm,d_index-1,n)#perm[:pre] are taken care of by other grouping
                        else:
                            pre=findpre(perm,d_index,n)
                            
                        if pre==d_index:
                            continue
                        else:
                            group=tuple([cols[i] for i in perm])
                            self.rollupQuery(group, pre, d_index, agg)
                            for j in range(d_index,pre,-1):
                                prefix=group[:j]
                                if division and division>=j:
                                    division=None
                                condition=' and '.join(['g_'+group[k]+'=0' if k<j else 'g_'+group[k]+'=1'
                                                        for k in range(d_index)])                              
                                fd=pd.read_sql('SELECT '+','.join(prefix)+','+agg+' FROM grouping WHERE '+condition,
                                               con=self.conn)                                
                                self.fitmodel(fd,prefix,a,agg,division)
                            self.dropRollup()
                    self.dropAgg()    
        
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
        
        
    def aggQuery(self, g, a, agg):
        group=",".join(["CAST("+num+" AS NUMERIC)" for num in self.num if num!=a and num in g]+
                        [cat for cat in self.cat if cat!=a and cat in g])
        if agg=='sum':
            a='CAST('+a+' AS NUMERIC)'
        query="CREATE TEMP TABLE agg as SELECT "+group+","+agg+"("+a+")"+" FROM "+self.table+" GROUP BY "+group
        self.conn.execute(query)
    
    def rollupQuery(self, group, pre, d_index, agg):
        grouping=",".join([attr+", GROUPING("+attr+") as g_"+attr for attr in group[:d_index]])
        gsets=','.join(['('+','.join(group[:prefix])+')' for prefix in range(d_index,pre,-1)])
        self.conn.execute('CREATE TEMP TABLE grouping AS '+
                        'SELECT '+grouping+', SUM('+agg+') as '+agg+
                        ' FROM agg GROUP BY GROUPING SETS('+gsets+')'+
                        ' ORDER BY '+','.join(group[:d_index]))
        
    def dropRollup(self):
        self.conn.execute('DROP TABLE grouping')
        
    def dropAgg(self):
        self.conn.execute('DROP TABLE agg')
        
    def fitmodel(self, fd, group, a, agg, division):  
        if division:
            self.fitmodel_with_division(fd, group, a, agg, division)
        else:
            self.fitmodel_no_division(fd, group, a, agg)
            
    def fitmodel_no_division(self, fd, group, a, agg):
        size=len(group)-1
        oldKey=None
        oldIndex=[0]*size
        num_f=[0]*size
        valid_l_f=[0]*size
        valid_c_f=[0]*size
        f=[list(group[:i]) for i in range(1,size+1)]
        v=[list(group[j:]) for j in range(1,size+1)]
        
        def fit(df,i,n):
            if not self.fit:
                return
            describe=[mean(df[agg]),mode(df[agg]),percentile(df[agg],25)
                      ,percentile(df[agg],50),percentile(df[agg],75)]
            
            fval=[getattr(oldKey,j) for j in f[i]]                    
            #fitting constant
            theta_c=chisquare(df[agg])[1]
            if theta_c>self.theta_c:
                nonlocal valid_c_f
                valid_c_f[i]+=1
                #self.pc.add_local(f,oldKey,v,a,agg,'const',theta_c)
                self.conn.execute(self.addLocal(f[i],fval,v[i],a,agg,'const',theta_c,describe,describe[0]))
              
            #fitting linear
            if  theta_c!=1 and all(attr in self.num for attr in v[i]):
                #=======================================================
                # lr=sm.ols(agg+'~'+'+'.join(v),data=df).fit()
                # theta_l=lr.rsquared_adj
                #=======================================================
                lr=LinearRegression()
                lr.fit(df[v[i]],df[agg])
                theta_l=lr.score(df[v[i]],df[agg])
                theta_l=1-(1-theta_l)*(n-1)/(n-len(v[i])-1)
                param=lr.coef_.tolist()
                param.append(lr.intercept_.tolist())
                if theta_l and theta_l>self.theta_l:
                    nonlocal valid_l_f
                    valid_l_f[i]+=1
                #self.pc.add_local(f,oldKey,v,a,agg,'linear',theta_l)
                    self.conn.execute(self.addLocal(f[i],fval,v[i],a,agg,'linear',theta_l,describe,param))
        
        for tup in fd.itertuples():
            position=None
            if oldKey:
                for i in range(size):
                    if getattr(tup,group[i])!=getattr(oldKey,group[i]):
                        position=i
                        break
            
            if position is not None:
                index=tup.Index
                for i in range(position,size):
                    temp=fd[oldIndex[i]:index]
                    num_f[i]+=1
                    n=index-oldIndex[i]
                    if n>len(v[i])+1:
                        fit(temp,i,n)
                    oldIndex[i]=index
                    
            oldKey=tup
            
        if oldKey:
            for i in range(size):
                temp=fd[oldIndex[i]:]
                num_f[i]+=1
                n=len(temp)
                if n>len(v[i])+1:
                    fit(temp,i,n)
        
        #sifting global            
        for i in range(size):
            lamb_c=valid_c_f[i]/num_f[i]
            lamb_l=valid_l_f[i]/num_f[i]
            if lamb_c>self.lamb:
                #self.pc.add_global(f,v,a,agg,'const',self.theta_c,lamb_c)
                self.conn.execute(self.addGlobal(f[i],v[i],a,agg,'const',self.theta_c,lamb_c))
            if lamb_l>self.lamb:
                #self.pc.add_global(f,v,a,agg,'linear',str(self.theta_l),str(lamb_l))
                self.conn.execute(self.addGlobal(f[i],v[i],a,agg,'linear',self.theta_l,lamb_l))
        
        if not self.fit:
            return        
        #adding local with f=empty set
        describe=[mean(fd[agg]),mode(fd[agg]),percentile(fd[agg],25)
                                          ,percentile(fd[agg],50),percentile(fd[agg],75)]
                           
        #fitting constant
        theta_c=chisquare(fd[agg])[1]
        if theta_c>self.theta_c:
            self.conn.execute(self.addLocal([' '],[' '],group,a,agg,'const',theta_c,describe,describe[0]))
          
        #fitting linear
        if  theta_c!=1 and all(attr in self.num for attr in group):
            lr=LinearRegression()
            gl=list(group)
            lr.fit(fd[gl],fd[agg])
            theta_l=lr.score(fd[gl],fd[agg])
            n=len(fd)
            theta_l=1-(1-theta_l)*(n-1)/(n-len(group)-1)
            param=lr.coef_.tolist()
            param.append(lr.intercept_.tolist())
            if theta_l and theta_l>self.theta_l:
            #self.pc.add_local(f,oldKey,v,a,agg,'linear',theta_l)
                self.conn.execute(self.addLocal([' '],[' '],group,a,agg,'linear',theta_l,describe,param))
                
    def fitmodel_with_division(self, fd, group, a, agg, division): 
        #fd=d.sort_values(by=f).reset_index(drop=True)
        oldKey=None
        oldIndex=0
        num_f=0
        valid_l_f=0
        valid_c_f=0
        f=list(group[:division])
        v=list(group[division:])
        l=0
        if all([attr in self.num for attr in v]):
            l=1
        #df:dataframe n:length    
        def fit(df,f,v,n):
            if not self.fit:
                return
            describe=[mean(df[agg]),mode(df[agg]),percentile(df[agg],25)
                                          ,percentile(df[agg],50),percentile(df[agg],75)]
            
            fval=[getattr(oldKey,j) for j in f]                    
            #fitting constant
            theta_c=chisquare(df[agg])[1]
            if theta_c>self.theta_c:
                nonlocal valid_c_f
                valid_c_f+=1
                #self.pc.add_local(f,oldKey,v,a,agg,'const',theta_c)
                self.conn.execute(self.addLocal(f,fval,v,a,agg,'const',theta_c,describe,describe[0]))
                
            #fitting linear
            if l==1 and theta_c!=1:
                #=======================================================
                # lr=sm.ols(agg+'~'+'+'.join(v),data=df).fit()
                # theta_l=lr.rsquared_adj
                #=======================================================
                lr=LinearRegression()
                lr.fit(df[v],df[agg])
                theta_l=lr.score(df[v],df[agg])
                theta_l=1-(1-theta_l)*(n-1)/(n-len(v)-1)
                param=lr.coef_.tolist()
                param.append(lr.intercept_.tolist())
                if theta_l and theta_l>self.theta_l:
                    nonlocal valid_l_f
                    valid_l_f+=1
                #self.pc.add_local(f,oldKey,v,a,agg,'linear',theta_l)
                    self.conn.execute(self.addLocal(f,fval,v,a,agg,'linear',theta_l,describe,param))
        
        for tup in fd.itertuples():
            if oldKey and any([getattr(tup,attr)!=getattr(oldKey,attr) for attr in f]):
                index=tup.Index
                temp=fd[oldIndex:index]
                num_f+=1
                n=index-oldIndex
                if n>len(v)+1:
                    fit(temp,f,v,n)                       
                oldIndex=index
            oldKey=tup
            
        if oldKey:
            temp=fd[oldIndex:]
            num_f+=1
            n=len(temp)
            if n>len(v)+1:
                fit(temp,f,v,n)
        
        lamb_c=valid_c_f/num_f
        lamb_l=valid_l_f/num_f
        if lamb_c>self.lamb:
            #self.pc.add_global(f,v,a,agg,'const',self.theta_c,lamb_c)
            self.conn.execute(self.addGlobal(f,v,a,agg,'const',self.theta_c,lamb_c))
        if lamb_l>self.lamb:
            #self.pc.add_global(f,v,a,agg,'linear',str(self.theta_l),str(lamb_l))
            self.conn.execute(self.addGlobal(f,v,a,agg,'linear',self.theta_l,lamb_l))
                          
    def addLocal(self,f,f_val,v,a,agg,model,theta,describe,param):
        f="'"+str(f).replace("'","")+"'"
        f_val="'"+str(f_val).replace("'","")+"'"
        v="'"+str(v).replace("'","")+"'"
        a="'"+a+"'"
        agg="'"+agg+"'"
        model="'"+model+"'"
        theta="'"+str(theta)+"'"
        describe="'"+str(describe).replace("'","")+"'"
        param="'"+str(param)+"'"
        return 'insert into '+self.table+'_local values('+','.join([f,f_val,v,a,agg,model,theta,describe,param])+');'
    
    
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
        self.conn.execute('DROP TABLE IF EXISTS '+self.table+'_local;')
        self.conn.execute('create table IF NOT EXISTS '+self.table+'_local('+
                     'fixed varchar,'+
                     'fixed_value varchar,'+
                     'variable varchar,'+
                     'in_a varchar,'+
                     'agg varchar,'+
                     'model varchar,'+
                     'theta float,'+
                     'stats varchar,'+
                     'param varchar);')
        
        self.conn.execute('DROP TABLE IF EXISTS '+self.table+'_global')
        self.conn.execute('create table IF NOT EXISTS '+self.table+'_global('+
                     'fixed varchar,'+
                     'variable varchar,'+
                     'in_a varchar,'+
                     'agg varchar,'+
                     'model varchar,'+
                     'theta float,'+
                     'lambda float);')
                
        self.conn.execute('create table IF NOT EXISTS time('+
                          'time varchar,'+
                          'description varchar);')
        
