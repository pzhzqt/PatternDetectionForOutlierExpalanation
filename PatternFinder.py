import pandas as pd
from itertools import combinations
#import statsmodels.formula.api as sm
from sklearn.linear_model import LinearRegression
from scipy.stats import chisquare,mode
from numpy import percentile,mean
from time import time
from permtest import *
from fd import closure
from numpy.f2py.auxfuncs import throw_error

class PatternFinder:
    conn=None
    table=None
    theta_c=None #theta for constant regression
    theta_l=None #theta for linear regression
    lamb=None #lambda
    cat=None #categorical attributes
    num=None #numeric attributes
    schema=None #list of all attributes
    n=None#number of attributes
    attr_index={} #index of all attributes
    grouping_attr=None #attributes can be in group
    #c_star=False
    #pc=None
    fit=None #if we are fitting model
    dist_thre=None #threshold for identifying distinct-value attributes
    time=None #record running time for each section
    fd={} #functional dependencies
    glob=[] #global patterns
    num_rows=None #number of rows of self.table
    
    def __init__(self, conn, table, fit=True, theta_c=0.75, theta_l=0.75, lamb=0.8, dist_thre=0.9):
        self.conn=conn
        self.theta_c=theta_c
        self.theta_l=theta_l
        self.lamb=lamb
        self.fit=fit
        self.dist_thre=dist_thre
        self.time={'aggregate':0,'df':0,'regression':0,'insertion':0,'drop':0,'loop':0,
                   'innerloop':0,'get_attr':0,'make_reference':0,'itertuples':0,'total':0}
        
        try:
            self.table=table
            self.schema=list(pd.read_sql("SELECT * FROM "+self.table+" LIMIT 1",self.conn))
        except Exception as ex:
            print(ex)
        
        self.n=len(self.schema)
        for i in range(self.n):
            self.attr_index[self.schema[i]]=i
        
        self.cat=[]
        self.num=[]
        self.grouping_attr=[]
        self.num_rows=pd.read_sql("SELECT count(*) as num from "+self.table,self.conn)['num'][0]
#         self.fd={}
        #check uniqueness, grouping_attr contains only non-unique attributes
        unique=pd.read_sql("SELECT attname,n_distinct FROM pg_stats WHERE tablename='"+table+"'",self.conn)
        for tup in unique.itertuples():
            if (tup.n_distinct<0 and tup.n_distinct > -self.dist_thre) or \
            (tup.n_distinct>0 and tup.n_distinct<self.num_rows*self.dist_thre):
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
    
    def setFd(self, fd):
        '''
        tyep fd:list of size-2 tuples, tuple[0]=list of lhs attributes and tuple[1]=list of rhs attributes
        '''
        for tup in fd:
            for i in range(2):
                for j in range(len(tup[i])):
                    try:
                        tup[i][j]=self.attr_index[tup[i][j]]
                    except KeyError as ex:
                        print(str(ex)+" is not in the table")
                        raise ex
        self.fd=fd
        
    def validateFd(self,group,division=None):
        '''
        check if we want to ignore f=group[:division], v=group[division:]
        type group: tuple of strings
        
        return True if group is valid
        return False if we decide to ignore it
        
        if division=None, check all possible divisions, and return boolean[]
        '''
        if division:
            f=set()# indices of fixed attributes
            attrs=set()# indices of all attributes
            for i in range(len(group)):
                if i<division:
                    f.add(self.attr_index[group[i]])
                attrs.add(self.attr_index[group[i]])
                
            for i in range(len(group)):
                cur=self.attr_index[group[i]]
                if i<division: #group[i] in f
                    if cur in closure(self.fd,self.n,f-{cur}):
                        return False
                else: #group[i] in v
                    if cur in closure(self.fd,self.n,attrs-{cur}):
                        return False
                    
            return True
        else:
            n=len(group)
            ret=[self.validateFd(group,i) for i in range(1,n)] #division from 1 to n-1
            return ret
            
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
                        
                        #check if perm[0]->perm[1], if so, ignore whole group
                        if perm[1] in closure(self.fd,self.n,[perm[0]]):
                            continue
                        
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
                                    
                                #check functional dependency here if division exists, otherwise check in fit
                                if division and not self.validateFd(prefix,division):
                                    continue
                                
                                condition=' and '.join(['g_'+group[k]+'=0' if k<j else 'g_'+group[k]+'=1'
                                                        for k in range(d_index)])
                                df_start=time()                              
                                df=pd.read_sql('SELECT '+','.join(prefix)+','+agg+' FROM grouping WHERE '+condition,
                                               con=self.conn)
                                self.time['df']+=time()-df_start                                
                                self.fitmodel(df,prefix,a,agg,division)
                            self.dropRollup()
                    self.dropAgg()    
        if self.glob:
            insert_start=time()
            self.conn.execute("INSERT INTO "+self.table+"_global values"+','.join(self.glob))
            self.time['insertion']+=time()-insert_start
        self.time['total']=time()-start
        self.insertTime()
        
        
#     def formCube(self, a, agg, attr):
#         group=",".join(["CAST("+num+" AS NUMERIC)" for num in self.num if num!=a and num in attr]+
#                         [cat for cat in self.cat if cat!=a and cat in attr])
#         grouping=",".join(["CAST("+num+" AS NUMERIC), GROUPING(CAST("+num+" AS NUMERIC)) as g_"+num
#                         for num in self.num if num!=a and num in attr]+
#             [cat+", GROUPING("+cat+") as g_"+cat for cat in self.cat if cat!=a and cat in attr])
#         if a in self.num:
#             a="CAST("+a+" AS NUMERIC)"
#         query="CREATE TABLE cube AS SELECT "+agg+"("+a+"), "+grouping+" FROM "+self.table+" GROUP BY CUBE("+group+")"
#         self.conn.execute(query)
#         indx=",".join(["g_"+col for col in attr])
#         self.conn.execute("CREATE INDEX in_a on cube("+indx+");")
#         
#     
#     def dropCube(self):
#         self.conn.execute("DROP TABLE cube;")
        
        
    def aggQuery(self, g, a, agg):
        start=time()
        group=",".join(["CAST("+num+" AS NUMERIC)" for num in self.num if num!=a and num in g]+
                        [cat for cat in self.cat if cat!=a and cat in g])
        if agg=='sum':
            a='CAST('+a+' AS NUMERIC)'
        query="CREATE TEMP TABLE agg as SELECT "+group+","+agg+"("+a+")"+" FROM "+self.table+" GROUP BY "+group
        self.conn.execute(query)
        self.time['aggregate']+=time()-start
    
    def rollupQuery(self, group, pre, d_index, agg):
        start=time()
        grouping=",".join([attr+", GROUPING("+attr+") as g_"+attr for attr in group[:d_index]])
        gsets=','.join(['('+','.join(group[:prefix])+')' for prefix in range(d_index,pre,-1)])
        self.conn.execute('CREATE TEMP TABLE grouping AS '+
                        'SELECT '+grouping+', SUM('+agg+') as '+agg+
                        ' FROM agg GROUP BY GROUPING SETS('+gsets+')'+
                        ' ORDER BY '+','.join(group[:d_index]))
        self.time['aggregate']+=time()-start
        
    def dropRollup(self):
        drop_start=time()
        self.conn.execute('DROP TABLE grouping')
        self.time['drop']+=time()-drop_start
        
    def dropAgg(self):
        drop_start=time()
        self.conn.execute('DROP TABLE agg')
        self.time['drop']+=time()-drop_start
        
    def fitmodel(self, fd, group, a, agg, division):
        loop_start=time()
        if division:
            self.fitmodel_with_division(fd, group, a, agg, division)
        else:
            self.fitmodel_no_division(fd, group, a, agg)
        self.time['loop']+=time()-loop_start
            
    def fitmodel_no_division(self, fd, group, a, agg):
        size=len(group)-1
        oldKey=None
        oldIndex=[0]*size
        num_f=[0]*size
        valid_l_f=[0]*size
        valid_c_f=[0]*size
        f=[list(group[:i]) for i in range(1,size+1)]
        v=[list(group[j:]) for j in range(1,size+1)]
        fd_valid=self.validateFd(group)
        if not any(fd_valid):
            return
        pattern=[]
        def fit(df,i,n):
            if not self.fit:
                return
            reg_start=time()
            describe=[mean(df[agg]),mode(df[agg]),percentile(df[agg],25)
                      ,percentile(df[agg],50),percentile(df[agg],75)]
            
            fval=[getattr(oldKey,j) for j in f[i]]                    
            #fitting constant
            theta_c=chisquare(df[agg])[1]
            if theta_c>self.theta_c:
                nonlocal valid_c_f
                valid_c_f[i]+=1
                #self.pc.add_local(f,oldKey,v,a,agg,'const',theta_c)
                pattern.append(self.addLocal(f[i],fval,v[i],a,agg,'const',theta_c,describe,describe[0]))
              
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
                    pattern.append(self.addLocal(f[i],fval,v[i],a,agg,'linear',theta_l,describe,param))
                    
            self.time['regression']+=time()-reg_start
            
        inner_loop_start=time()
        iter_start=time()
        for tup in fd.itertuples():
            self.time['itertuples']+=time()-iter_start
            position=None
            if oldKey:
                get_attr=time()
                for i in range(size):
                    if getattr(tup,group[i])!=getattr(oldKey,group[i]):
                        position=i
                        break
                self.time['get_attr']+=time()-get_attr
            
            if position is not None:
                index=tup.Index
                for i in range(position,size):
#                     make_reference=time()
#                     temp=fd[oldIndex[i]:index].copy()
#                     self.time['make_reference']+=time()-make_reference
                    num_f[i]+=1
                    n=index-oldIndex[i]
                    if n>len(v[i])+1 and fd_valid[i]:
                        fit(fd[oldIndex[i]:index],i,n)
                    oldIndex[i]=index
                    
            oldKey=tup
            iter_start=time()
            
        if oldKey:
            for i in range(size):
#                 make_reference=time()
#                 temp=fd[oldIndex[i]:].copy()
#                 self.time['make_reference']+=time()-make_reference
                num_f[i]+=1
                n=oldKey.Index-oldIndex[i]
                if n>len(v[i])+1 and fd_valid[i]:
                    fit(fd[oldIndex[i]:],i,n)
        self.time['innerloop']+=time()-inner_loop_start
        
        #sifting global            
        for i in range(size):
            lamb_c=valid_c_f[i]/num_f[i]
            lamb_l=valid_l_f[i]/num_f[i]
            if lamb_c>self.lamb:
                #self.pc.add_global(f,v,a,agg,'const',self.theta_c,lamb_c)
                self.glob.append(self.addGlobal(f[i],v[i],a,agg,'const',self.theta_c,lamb_c))
            if lamb_l>self.lamb:
                #self.pc.add_global(f,v,a,agg,'linear',str(self.theta_l),str(lamb_l))
                self.glob.append(self.addGlobal(f[i],v[i],a,agg,'linear',self.theta_l,lamb_l))
        
        if not self.fit:
            return 
        
        #adding local with f=empty set
        if not self.validateFd(group,0):
            return
        reg_start=time()
        describe=[mean(fd[agg]),mode(fd[agg]),percentile(fd[agg],25)
                                          ,percentile(fd[agg],50),percentile(fd[agg],75)]
                           
        #fitting constant
        theta_c=chisquare(fd[agg])[1]
        if theta_c>self.theta_c:
            pattern.append(self.addLocal([' '],[' '],group,a,agg,'const',theta_c,describe,describe[0]))
          
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
                pattern.append(self.addLocal([' '],[' '],group,a,agg,'linear',theta_l,describe,param))
        self.time['regression']+=time()-reg_start
        
        if pattern:
            insert_start=time()
            self.conn.execute("INSERT INTO "+self.table+"_local values"+','.join(pattern))        
            self.time['insertion']+=time()-insert_start
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
        pattern=[]
        def fit(df,f,v,n):
            if not self.fit:
                return
            reg_start=time()
            describe=[mean(df[agg]),mode(df[agg]),percentile(df[agg],25)
                                          ,percentile(df[agg],50),percentile(df[agg],75)]
            
            fval=[getattr(oldKey,j) for j in f]                    
            #fitting constant
            theta_c=chisquare(df[agg])[1]
            if theta_c>self.theta_c:
                nonlocal valid_c_f
                valid_c_f+=1
                #self.pc.add_local(f,oldKey,v,a,agg,'const',theta_c)
                pattern.append(self.addLocal(f,fval,v,a,agg,'const',theta_c,describe,describe[0]))
                
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
                    pattern.append(self.addLocal(f,fval,v,a,agg,'linear',theta_l,describe,param))
                    
            self.time['regression']+=time()-reg_start
        
        inner_loop_start=time()
        iter_start=time()
        change=False
        for tup in fd.itertuples():
            self.time['itertuples']+=time()-iter_start
            if oldKey:
                get_attr=time()
                change=any([getattr(tup,attr)!=getattr(oldKey,attr) for attr in f])
                self.time['get_attr']+=time()-get_attr
            if change:
                index=tup.Index
#                 make_reference=time()
#                 temp=fd[oldIndex:index].copy()
#                 self.time['make_reference']+=time()-make_reference
                num_f+=1
                n=index-oldIndex
                if n>len(v)+1:
                    fit(fd[oldIndex:index],f,v,n)                       
                oldIndex=index
            oldKey=tup
            iter_start=time()
            
        if oldKey:
#             make_reference=time()
#             temp=fd[oldIndex:].copy()
#             self.time['make_reference']+=time()-make_reference
            num_f+=1
            n=oldKey.Index-oldIndex
            if n>len(v)+1:
                fit(fd[oldIndex:],f,v,n)
        self.time['innerloop']+=time()-inner_loop_start
        
        if pattern:
            insert_start=time()
            self.conn.execute("INSERT INTO "+self.table+"_local values"+','.join(pattern))
            self.time['insertion']+=time()-insert_start
        
        lamb_c=valid_c_f/num_f
        lamb_l=valid_l_f/num_f
        if lamb_c>self.lamb:
            #self.pc.add_global(f,v,a,agg,'const',self.theta_c,lamb_c)
            self.glob.append(self.addGlobal(f,v,a,agg,'const',self.theta_c,lamb_c))
        if lamb_l>self.lamb:
            #self.pc.add_global(f,v,a,agg,'linear',str(self.theta_l),str(lamb_l))
            self.glob.append(self.addGlobal(f,v,a,agg,'linear',self.theta_l,lamb_l))
                          
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
        #return 'insert into '+self.table+'_local values('+','.join([f,f_val,v,a,agg,model,theta,describe,param])+');'
        return '('+','.join([f,f_val,v,a,agg,model,theta,describe,param])+')'
    
    
    def addGlobal(self,f,v,a,agg,model,theta,lamb):
        f="'"+str(f).replace("'","")+"'"
        v="'"+str(v).replace("'","")+"'"
        a="'"+a+"'"
        agg="'"+agg+"'"
        model="'"+model+"'"
        theta="'"+str(theta)+"'"
        lamb="'"+str(lamb)+"'"
        #return 'insert into '+self.table+'_global values('+','.join([f,v,a,agg,model,theta,lamb])+');'
        return '('+','.join([f,v,a,agg,model,theta,lamb])+')'
         
    
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
        
        attr=''
        for key in self.time:
            attr+=key+' varchar,'
        self.conn.execute('create table IF NOT EXISTS time_detail('+
                          'id serial primary key,'+
                          attr+
                          'description varchar);')
        
        
    def insertTime(self):
        attributes=list(self.time)
        values=[str(self.time[i]) for i in attributes]
        self.conn.execute('INSERT INTO time_detail('+','.join(attributes)+') values('+','.join(values)+')')
