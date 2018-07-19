import pandas as pd
from itertools import combinations
import statsmodels.formula.api as sm
from psycopg2.extras import Json
from sklearn.linear_model import LinearRegression
from scipy.stats import chisquare,mode
from numpy import percentile,mean
from time import time
from permtest import *
from fd import closure
from random import shuffle
from math import ceil

class PatternFinder:
    conn=None
    table=None
    theta_c=None #theta for constant regression
    theta_l=None #theta for linear regression
    lamb=None #lambda
    summable=None #attributes that are summable
    num=None #numeric attributes
    schema=None #list of all attributes
    n=None#number of attributes
    attr_index=None #index of all attributes
    grouping_attr=None #attributes can be in group
    #c_star=False
    #pc=None
    fit=None #if we are fitting model
    dist_thre=None #threshold for identifying distinct-value attributes
    time=None #record running time for each section
    fd=None #functional dependencies
    glob=None #global patterns
    num_rows=None #number of rows of self.table
    reg_package=None #statsmodels or sklearn
    supp_l=None #local support
    supp_g=None #global support
    failedf=None #used to apply support inference
    superkey=None #used to track compound key
    fd_check=None #toggle on/off functional dependency checks
    supp_inf=None #toggle on/off support inference rules
    algorithm=None #{'optimized','naive','naive_alternative'}
    pattern_schema=None #schema for storing pattern
    group_rows=None #track # of rows for each group
    sampling=None
    
    def __init__(self, conn, table, fit=True, sampling=False, theta_c=0.5, theta_l=0.5, lamb=0.5, dist_thre=0.9,
                 reg_package='statsmodels',supp_l=15,supp_g=15,fd_check=False,supp_inf=False,algorithm='test',
                 pattern_schema='dev'):
        self.sampling=sampling
        self.conn=conn
        self.theta_c=theta_c
        self.theta_l=theta_l
        self.lamb=lamb
        self.fit=fit
        self.dist_thre=dist_thre
        if reg_package not in {'statsmodels','sklearn'}:
            print('Invalid input for reg_package, reset to default')
            reg_package='sklearn'
        self.reg_package=reg_package
        self.supp_l=supp_l
        self.supp_g=supp_g
        self.pattern_schema=pattern_schema
        self.superkey=set()
        self.fd_check=fd_check
        self.fd=[]
        self.supp_inf=supp_inf
        if algorithm not in {'naive','naive_alternative','optimized','test','bruteforce'}:
            print('Invalid input for algorithm, reset to default')
            algorithm='test'
        self.algorithm=algorithm
        self.time={'aggregate':0,'df':0,'regression':0,'insertion':0,'drop':0,'loop':0,
                   'innerloop':0,'fd_detect':0,'check_fd':0,'total':0}
        
        try:
            self.table=table
            self.schema=list(pd.read_sql("SELECT * FROM "+self.table+" LIMIT 1",self.conn))
        except Exception as ex:
            print(ex)
        
        self.n=len(self.schema)
        
        self.attr_index={}
        for i in range(self.n):
            self.attr_index[self.schema[i]]=i
        
        self.num=[]
        self.grouping_attr=[]
        self.num_rows=pd.read_sql("SELECT count(*) as num from "+self.table,self.conn)['num'][0]
#         self.fd={}
        #check uniqueness, grouping_attr contains only non-unique attributes
        unique=pd.read_sql("SELECT "+','.join(["count(distinct "+attr+") as "+attr for attr in self.schema])+ " FROM "+self.table,
                                              self.conn)                                        
        self.group_rows={}
        for attr in self.schema:
            #if (tup.n_distinct<0 and tup.n_distinct > -self.dist_thre) or \
            #(tup.n_distinct>0 and tup.n_distinct<self.num_rows*self.dist_thre):
             #   self.grouping_attr.append(tup.attname)
             
            #aggresive approach:
            n_distinct=unique[attr][0]
            if n_distinct>=self.num_rows*self.dist_thre:
                continue
            self.grouping_attr.append(attr)
            self.group_rows[frozenset([attr])]=n_distinct
                
        for col in self.schema:
#             if col=='year':
#                 self.num.append(col)
#             elif col!='id':
#                 self.cat.append(col)
            try:
                self.conn.execute("SELECT CAST("+col+" AS NUMERIC) FROM "+self.table)
                self.num.append(col)
            except:
                continue
        self.summable=self.num
        
    def setNumeric(self):
        print("Current Numeric: ")
        print(self.num)
        if input('Redefine? (y/n, default n)')=='y':
            num=[]
            for attr in self.num:
                keep=input('Keep '+attr+' as Numeric?(y/n,default y)')
                if keep=='n':
                    continue
                else:
                    num.append(attr)
            self.num=num
            self.summable=[attr for attr in self.summable if attr in num]
        
    def setSummable(self):
        print("Current Summable: ")
        print(self.summable)
        if input('Redefine? (y/n, default n)')=='y':
            summable=[]
            for attr in self.summable:
                keep=input('Keep '+attr+' as Summable?(y/n,default y)')
                if keep=='n':
                    continue
                else:
                    summable.append(attr)
            self.summable=summable
    
    def addFd(self, fd):
        '''
        type fd:list of size-2 tuples, tuple[0]=list of lhs attributes and tuple[1]=list of rhs attributes
        '''
        if not self.fd_check: #if toggle is off, not adding anything
            return
        for tup in fd:
            for i in range(2):
                for j in range(len(tup[i])):
                    try:
                        tup[i][j]=self.attr_index[tup[i][j]]
                    except KeyError as ex:
                        print(str(ex)+" is not in the table")
                        raise ex
        self.fd.extend(fd)
        
    def validateFd(self,group,division=None):
        '''
        check if we want to ignore f=group[:division], v=group[division:]
        type group: tuple of strings
        
        return True if group is valid
        return False if we decide to ignore it
        
        if division=None, check all possible divisions, and return boolean[]
        '''
        if division:
#             if not self.fd:
#                 return True
#             f=set()# indices of fixed attributes
#             attrs=set()# indices of all attributes
#             for i in range(len(group)):
#                 if i<division:
#                     f.add(self.attr_index[group[i]])
#                 attrs.add(self.attr_index[group[i]])
#                 
#             for i in range(len(group)):
#                 cur=self.attr_index[group[i]]
#                 if i<division: #group[i] in f
#                     if cur in closure(self.fd,self.n,f-{cur}):
#                         return False
#                 else: #group[i] in v
#                     if cur in closure(self.fd,self.n,attrs-{cur}):
#                         return False
#                     
#             return True
            if not self.fd_check:
                return True
            if division==1:
                return True
            row=self.group_rows[frozenset(group[:division])]
            for i in group[:division]:
                if self.group_rows[frozenset([k for k in group[:division] if k!=i])]>=row*self.dist_thre:
#                    print(frozenset([k for k in group[:division] if k!=i]))
#                    print(i)
                    return False
            return True
        else:
            n=len(group)
            ret=[self.validateFd(group,i) for i in range(1,n)] #division from 1 to n-1
            return ret
            
    def findPattern(self,user=None):
#       self.pc=PC.PatternCollection(list(self.schema))
        self.glob=[]#reset self.glob
        self.createTable(self.pattern_schema)
        start=time()
        if not user:
            grouping_attr=self.grouping_attr
            aList=self.summable+['*']
            
        else:
            grouping_attr=user['group']
            aList=user['a']
        if self.algorithm=='bruteforce':
            for Fsize in range(1,min(4,self.n)):
                for F in combinations(self.schema,Fsize):
                    df_start=time()
                    fs=pd.read_sql('SELECT '+','.join(F)+' FROM '+self.table+' GROUP BY '+','.join(F),self.conn)
                    self.time['df']+=time()-df_start
                    for Vsize in range(1,min(4,self.n)-Fsize+1):
                        for V in combinations([attr for attr in self.schema if attr not in F],Vsize):
                            for a in aList:
                                if a in F or a in V:
                                    continue
                                if a=='*':
                                    agg='count'
                                    aggPattern='count'
                                else:
                                    agg='sum'
                                    a= 'CAST ('+a+' AS NUMERIC)'
                                    aggPattern='sum_'+a
                                num_f=0
                                valid_c_f=0
                                valid_l_f=0
                                pattern=[]
                                for f in fs.itertuples():
                                    if None in f[1:]:
                                        continue
                                    df_start=time()
                                    fval=str(f[1:])
                                    if Fsize==1:
                                        fval=fval.replace(",", "")
                                    df=pd.read_sql('SELECT '+','.join(F)+','+','.join(V)+','+agg+'('+a+') FROM '+self.table+
                                                   ' WHERE ('+','.join(F)+')='+fval+' GROUP BY '+
                                                   ','.join(F)+','+','.join(V),self.conn)
                                    self.time['df']+=time()-df_start                                   
                                    n=len(df)
                                    if n<self.supp_l:
                                        continue
                                    
                                    reg_start=time()
                                    describe=[mean(df[agg]),mode(df[agg]),percentile(df[agg],25)
                                              ,percentile(df[agg],50),percentile(df[agg],75)]
                                    num_f+=1 
                                    #fitting constant
                                    theta_c=chisquare(df[agg].dropna())[1]
                                    if theta_c>self.theta_c:
                                        valid_c_f+=1
                                        #self.pc.add_local(f,oldKey,v,a,agg,'const',theta_c)
                                        pattern.append(self.addLocal(F,f,V,aggPattern,'const',theta_c,describe,'NULL'))
                                    #fitting linear
                                    if  theta_c!=1 and ((self.reg_package=='sklearn' and all(attr in self.num for attr in V)
                                                        or
                                                        (self.reg_package=='statsmodels' and all(attr in self.num for attr in V)))):
                        
                                        if self.reg_package=='sklearn': 
                                            lr=LinearRegression()
                                            lr.fit(df[V],df[agg])
                                            theta_l=lr.score(df[V],df[agg])
                                            theta_l=1-(1-theta_l)*(n-1)/(n-len(v)-1)
                                            param=lr.coef_.tolist()
                                            param.append(lr.intercept_.tolist())
                                            param="'"+str(param)+"'"
                                        else: #statsmodels
                                            #num_cat=1
                                            #for attr in v:
                                                #if attr not in self.num:
                                                    #num_cat*=self.group_rows[frozenset([attr])]
                                            #if num_cat>500:
                                                #return
                                            
                                            lr=sm.ols(agg+'~'+'+'.join([attr if attr in self.num else 'C('+attr+')' for attr in V]),
                                                      data=df,missing='drop').fit()
                                            #theta_l=lr.rsquared_adj
                                            theta_l=chisquare(df[agg],lr.predict())[1]
                                            param=Json(dict(lr.params))
                                            
                                        if theta_l and theta_l>self.theta_l:
                                            valid_l_f+=1
                                        #self.pc.add_local(f,oldKey,v,a,agg,'linear',theta_l)
                                            pattern.append(self.addLocal(F,f,V,aggPattern,'linear',theta_l,describe,param))
                                    self.time['regression']+=time()-reg_start
                                if pattern:
                                    insert_start=time()
                                    self.conn.execute("INSERT INTO "+self.pattern_schema+"."+self.table+"_local values"+','.join(pattern))
                                    self.time['insertion']+=time()-insert_start
                                
                                if num_f:
                                    lamb_c=valid_c_f/num_f
                                    if valid_c_f>=self.supp_g and lamb_c>self.lamb:
                                        #self.pc.add_global(f,v,a,agg,'const',self.theta_c,lamb_c)
                                        self.glob.append(self.addGlobal(F,V,aggPattern,'const',self.theta_c,lamb_c))
                                            
                                    lamb_l=valid_l_f/num_f
                                    if valid_l_f>=self.supp_g and lamb_l>self.lamb:
                                        #self.pc.add_global(f,v,a,agg,'linear',str(self.theta_l),str(lamb_l))
                                        self.glob.append(self.addGlobal(F,V,aggPattern,'linear',self.theta_l,lamb_l))
        elif self.algorithm=='naive':
            for a in aList:
                if not user:
                    if a=='*':#For count, only do a count(*)
                        agg="count"
                    else: #a in self.num
                        agg="sum"
                else:
                    agg=user['agg']
                #for agg in aggList :
                cols=[col for col in grouping_attr if col!=a]
                n=len(cols)
                self.formCube(a, agg, cols)
                for size in range(min(4,n),1,-1):
                    combs=combinations(cols,size)
                    for group in combs:#comb=f+v
                        for fsize in range(1,len(group)):
                            fs=combinations(group,fsize)
                            for f in fs:
                                self.fit_naive(f,group,a,agg,cols)
                self.dropCube()
        elif self.algorithm=='naive_alternative' or self.algorithm=='test':
            self.failedf=set()
            for size in range(2,min(4,len(grouping_attr))+1):
                for group in combinations(grouping_attr,size):
                    #check if group contains superkey
                    for i in self.superkey:
                        if set(i).issubset(group):
                            break
                    else:#doesn't contain superkey
                        aggList=[]
                        for a in aList:
                            if a in group:
                                continue
                            if a=='*':
                                aggList.append('count')
                            else:
                                aggList.append('sum_'+a+'')
                        if len(aggList)==0:
                            continue
                        self.aggQuery(group, aList)
                        fd_detect_start=time()
                        isSuperkey=False
                        if self.fd_check:
                            cur_rows=pd.read_sql('SELECT count(*) as num FROM agg',con=self.conn)['num'][0]
                            if cur_rows>=self.num_rows*self.dist_thre:
                                self.superkey.add(group)
                                isSuperkey=True
                            self.group_rows[frozenset(group)]=cur_rows
                        self.time['fd_detect']+=time()-fd_detect_start
                        if not isSuperkey:
                            if self.algorithm=='naive_alternative':
                                for fsize in range(size-1,0,-1):
                                    for f in combinations(group,fsize):
                                        df_start=time()
                                        df=pd.read_sql('SELECT * FROM agg ORDER BY '+','.join(f),con=self.conn)
                                        self.time['df']+=time()-df_start
                                        grouping=tuple([fattr for fattr in f]+[vattr for vattr in group if vattr not in f])
                                        division=len(f)
                                        self.fitmodel(df, grouping, aggList, division)
                            else:#algorithm=='test'
                                for i in range(len(group)):
                                    perm=group[i:]+group[:i]
                                    df_start=time()
                                    df=pd.read_sql('SELECT * FROM agg ORDER BY '+','.join(perm[:-1]),con=self.conn)
                                    self.time['df']+=time()-df_start
                                    self.fitmodel(df, perm, aggList, None)
                                if len(group)==4:
                                    for f in [(group[0],group[2]),(group[1],group[3])]:
                                        grouping=tuple([fattr for fattr in f]+[vattr for vattr in group if vattr not in f])
                                        division=len(f)
                                        fd_check_start=time()
                                        fd_val=self.validateFd(grouping, division)
                                        self.time['check_fd']+=time()-fd_check_start
                                        if fd_val:
                                            df_start=time()
                                            df=pd.read_sql('SELECT * FROM agg ORDER BY '+','.join(f),con=self.conn)
                                            self.time['df']+=time()-df_start
                                            self.fitmodel(df, grouping, aggList, division)
                    self.dropAgg()
        else:#self.algorithm=='optimized'
            cols=[col for col in grouping_attr]
            n=len(cols)
            #test_start
#             all_group={}
#             for size in range(min(4,n),1,-1):
#                 for comb in combinations(cols,size):
#                     for fsize in range(size-1,0,-1):
#                         for f in combinations(comb,fsize):
#                             if f not in all_group:
#                                 all_group[f]=set()
#                             all_group[f].add(tuple([v for v in comb if v not in f]))
#                             
#             fit_group={}
            #test_end
            if user:
                up=n
            else:#without user question, need to make sure some attribute is left out for aggregate
                up=n-1
            combs=combinations([i for i in range(n)],min(4,up))
            for comb in combs:
                grouping=[cols[i] for i in comb]
                #self.aggQuery(grouping, aList)
                perms=permutations(comb,len(comb))
                for perm in perms:
                    self.failedf=set()#reset failed f for each permutation
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
                                
                        self.rollupQuery(group, pre, d_index, aList)
                        group_size=d_index#d_index might change
                        fd_detect_start=time()
                        if self.fd_check:
                            prev_rows=None
                            for j in range(pre,d_index+1):#first loop is to set prev_rows
                                condition=' and '.join(['g_'+group[k]+'=0' if k<j else 'g_'+group[k]+'=1'
                                                        for k in range(d_index)])
                                cur_rows=pd.read_sql('SELECT count(*) as num FROM grouping WHERE '+condition,
                                               con=self.conn)['num'][0]
                                if prev_rows:
                                    if cur_rows>=self.num_rows*self.dist_thre:
                                        d_index=j-1 #group[:j] will be distinct value groups (superkey)
                                        #self.addFd([group[:j-1],group[j-1]])
                                        self.superkey.add(group[:j])
                                        break
                                    elif prev_rows>=cur_rows*self.dist_thre:
                                        d_index=j-1#group[:j-1] implies group[j-1]
                                        self.addFd([(list(group[:j-1]),[group[j-1]])])
                                        break
                                prev_rows=cur_rows
                        self.time['fd_detect']+=time()-fd_detect_start
                            
                        for j in range(d_index,pre,-1):
                            prefix=group[:j]
                            
                            #check if group contains superkey
                            for i in self.superkey:
                                if set(prefix).issubset(i):
                                    break
                            else:# if contains superkey, go to next j
                                if division and division>=j:
                                    division=None
                                    
                                #check functional dependency here if division exists, otherwise check in fit
                                check_fd_start=time()
                                if division and not self.validateFd(prefix,division):
                                    continue
                                self.time['check_fd']+=time()-check_fd_start
                                
                                condition=' and '.join(['g_'+group[k]+'=0' if k<j else 'g_'+group[k]+'=1'
                                                        for k in range(group_size)])
                                df_start=time()
                                aggList=[]
                                for a in aList:
                                    if a in prefix:
                                        continue
                                    if a=='*':
                                        aggList.append('count(*)')
                                    else:
                                        aggList.append('sum('+a+')')
                                agg=['\"'+func+'\"' for func in aggList]                              
                                df=pd.read_sql('SELECT '+','.join(prefix)+','+','.join(agg)+' FROM grouping WHERE '+condition,
                                               con=self.conn)

                                self.time['df']+=time()-df_start                                
                                self.fitmodel(df,prefix,aggList,division)
                                #test start
                                #self.addTestGroup(fit_group,prefix,division)
                                #test end
                        self.dropRollup()
                #self.dropAgg()
            #test start
#             dif={}
#             for key in all_group:
#                 if key not in fit_group:
#                     dif[key]=all_group[key]
#                 else:
#                     for v in all_group[key]:
#                         if v not in fit_group[key]:
#                             if key not in dif:
#                                 dif[key]=set()
#                             dif[key].add(v)
#             print(dif)
            #test end
        if self.glob:
            insert_start=time()
            self.conn.execute("INSERT INTO "+self.pattern_schema+"."+self.table+"_global values"+','.join(self.glob))
            self.time['insertion']+=time()-insert_start
        self.time['total']=time()-start
        self.insertTime('\''+self.table+'_num_global:'+str(len(self.glob))+' algo:'+self.algorithm+'\'')
        
        
    def formCube(self, a, agg, attr):
        cube_start=time()
        group=",".join(["CAST("+num+" AS NUMERIC)" for num in attr if num in self.num]+
                        [cat for cat in attr if cat not in self.num])
        grouping=",".join(["GROUPING(CAST("+num+" AS NUMERIC)) as g_"+num
                        for num in attr if num in self.num]+
            ["GROUPING("+cat+") as g_"+cat for cat in attr if cat not in self.num])
        if a in self.num:
            qa="CAST("+a+" AS NUMERIC)"
        else:
            qa=a
        self.conn.execute("DROP TABLE IF EXISTS perform.cube")
        if agg=='count':
            name='count'
        else:
            name='sum_'+a
        query="CREATE TABLE perform.cube AS SELECT "+agg+"("+qa+") AS \""+name+"\", "+group+','+grouping+" FROM "+self.table+"\
        "+"GROUP BY CUBE("+group+") having "+'+'.join(['grouping('+att+')' if att not in self.num 
                                else "GROUPING(CAST("+att+" AS NUMERIC))" for att in attr])+'>='+str(len(attr)-4)
        
        self.conn.execute(query)    
        self.time['aggregate']+=time()-cube_start
     
    def dropCube(self):
        drop_start=time()
        self.conn.execute("DROP TABLE perform.cube;")
        self.time['drop']+=time()-drop_start
        
    def cubeQuery(self, g, f, cols):
        #=======================================================================
        # res=" and ".join([a+".notna()" for a in g])
        # if len(g)<len(cols):
        #     null=" and ".join([b+".isna()" for b in cols if b not in g])
        #     res=res+" and "+null
        #=======================================================================
        res=" and ".join(["g_"+a+"=0" for a in g])
        if len(g)<len(cols):
            unused=" and ".join(["g_"+b+"=1" for b in cols if b not in g])
            res=res+" and "+unused
        return "SELECT * FROM perform.cube where "+res+" ORDER BY "+",".join(f)
    
    def fit_naive(self,f,group,a,agg,cols):
        self.failedf=set()#to not trigger error
        df_start=time()
        fd=pd.read_sql(self.cubeQuery(group, f, cols),self.conn)
        self.time['df']+=time()-df_start
        g=tuple([att for att in f]+[attr for attr in group if attr not in f])
        division=len(f)
        if a=='*':
            aggList=['count']
        else:
            aggList=[agg+'_'+a]
        self.fitmodel_with_division(fd, g, aggList, division)
        
    def findPattern_inline(self,group,a,agg):
        #loop through permutations of group
        user={'group':group,'a':[a],'agg':agg}
        self.findPattern(user)
        
    def aggQuery(self, g, aList):
        start=time()
        group=",".join(["CAST("+att+" AS NUMERIC)" if att in self.num else att for att in g])
        agg=[]
        for a in aList:
            if a in g:
                continue
            if a=='*':
                agg.append('count(*) AS \"count\"')
            else:
                agg.append('sum(CAST('+a+' AS NUMERIC)) AS \"sum_'+a+'\"')
        query="CREATE TEMP TABLE agg as SELECT "+group+","+','.join(agg)+" FROM "+self.table+" GROUP BY "+group
        self.conn.execute(query)
        self.time['aggregate']+=time()-start
    
    def rollupQuery(self, group, pre, d_index, aList):
        start=time()
        grouping=",".join([attr+", GROUPING("+attr+") as g_"+attr for attr in group[:d_index]])
#        gsets=','.join(['('+','.join(group[:prefix])+')' for prefix in range(d_index,pre,-1)])
        ind=','.join(['g_'+attr for attr in group[:d_index]])
        #agg=['SUM(\"'+func+'\") AS '+'\"'+func+'\"' for func in aggList]
        agg=[]
        for a in aList:
            if a=='*':
                agg.append('count(*) AS \"count(*)\"')
            else:
                agg.append('sum(CAST('+a+' AS NUMERIC)) AS \"sum('+a+')\"')
        self.conn.execute('CREATE TEMP TABLE grouping AS '+
                        'SELECT '+grouping+','+','.join(agg)+
#                        ' FROM agg GROUP BY GROUPING SETS('+gsets+')'+
                        ' FROM '+self.table+' GROUP BY ROLLUP('+','.join(group[:d_index])+')'+
                        ' ORDER BY '+','.join(group[:d_index-1]))
        self.time['aggregate']+=time()-start
        
    def dropRollup(self):
        drop_start=time()
        self.conn.execute('DROP TABLE if exists grouping')
        self.time['drop']+=time()-drop_start
        
    def dropAgg(self):
        drop_start=time()
        self.conn.execute('DROP TABLE if exists agg')
        self.time['drop']+=time()-drop_start
        
    def fitmodel(self, fd, group, aggList, division):
        loop_start=time()
        if division:
            self.fitmodel_with_division(fd, group, aggList, division)
        else:
            self.fitmodel_no_division(fd, group, aggList)
        self.time['loop']+=time()-loop_start
            
    def fitmodel_no_division(self, fd, group, aggList):
        size=len(group)-1
        oldKey=None
        oldIndex=[0]*size
        num_f=[0]*size #fixed value number that satisfy support threshold
        valid_l_f=[{} for i in range(size)] #empty list to store valid fixed value number
        valid_c_f=[{} for i in range(size)]
        f=[list(group[:i]) for i in range(1,size+1)]
        v=[list(group[j:]) for j in range(1,size+1)]
        supp_valid=[group[:i] not in self.failedf for i in range(1,size+1)]
        f_dict=[{} for i in range(1,size+1)]
        check_fd_start=time()
        fd_valid=self.validateFd(group)
        sample_invalid=[{} for i in range(size)]
        global_dev_pos=[{} for i in range(size)]
        global_dev_neg=[{} for i in range(size)]
        for i in range(size):
            for agg in aggList:
                global_dev_pos[i][agg]={}
                global_dev_pos[i][agg]['l']=float('-inf')
                global_dev_pos[i][agg]['c']=float('-inf')
                global_dev_neg[i][agg]={}
                global_dev_neg[i][agg]['l']=float('inf')
                global_dev_neg[i][agg]['c']=float('inf')
        self.time['check_fd']+=time()-check_fd_start
        
        if not any(fd_valid) or not any(supp_valid):
            return
        pattern=[]
        def fit(df,fval,i,n):
            if not self.fit:
                return
            reg_start=time()
            for agg in aggList:
                nonlocal global_dev_pos,global_dev_neg
                if agg not in sample_invalid[i] or 'c' not in sample_invalid[i][agg]:
                    avg=mean(df[agg])
                    describe=[avg,mode(df[agg]),percentile(df[agg],25)
                              ,percentile(df[agg],50),percentile(df[agg],75)]
                    dev_pos=max(df[agg])-avg
                    dev_neg=min(df[agg])-avg
                    #fitting constant
                    theta_c=chisquare(df[agg].dropna())[1]
                    if theta_c>self.theta_c:
                        nonlocal valid_c_f
                        try:
                            valid_c_f[i][agg]+=1
                        except KeyError:
                            valid_c_f[i][agg]=1
                        global_dev_pos[i][agg]['c']=max(global_dev_pos[i][agg]['c'],dev_pos)
                        global_dev_neg[i][agg]['c']=min(global_dev_neg[i][agg]['c'],dev_neg)
                        #self.pc.add_local(f,oldKey,v,a,agg,'const',theta_c)
                        pattern.append(self.addLocal(f[i],fval,v[i],agg,'const',theta_c,describe,'NULL',
                                                     dev_pos,dev_neg))
                  
                #fitting linear
                if agg not in sample_invalid[i] or 'l' not in sample_invalid[i][agg]:
                    if  theta_c!=1 and ((self.reg_package=='sklearn' and all(attr in self.num for attr in v[i])
                                        or
                                        (self.reg_package=='statsmodels' and all(attr in self.num for attr in v[i])))):
        
                        if self.reg_package=='sklearn':   
                            lr=LinearRegression()
                            lr.fit(df[v[i]],df[agg])
                            theta_l=lr.score(df[v[i]],df[agg])
                            theta_l=1-(1-theta_l)*(n-1)/(n-len(v[i])-1)
                            param=lr.coef_.tolist()
                            param.append(lr.intercept_.tolist())
                            param="'"+str(param)+"'"
                        else: #statsmodels
                            if n<=len(v[i])+1:#negative R^2 for sure
                                return
                            #num_cat=1
                            #for attr in v[i]:
                                #if attr not in self.num:
                                    #num_cat*=self.group_rows[frozenset([attr])]
                            #if num_cat>500:
                                #return
                            lr=sm.ols(agg+'~'+'+'.join([attr if attr in self.num else 'C('+attr+')' for attr in v[i]])
                                      ,data=df,missing='drop').fit()
                            #theta_l=lr.rsquared_adj
                            theta_l=chisquare(df[agg],lr.predict())[1]
                            param=Json(dict(lr.params))
                            dev_pos=max(lr.resid)
                            dev_neg=min(lr.resid)
                        
                        if theta_l and theta_l>self.theta_l:
                            nonlocal valid_l_f
                            try:
                                valid_l_f[i][agg]+=1
                            except KeyError:
                                valid_l_f[i][agg]=1
                            global_dev_pos[i][agg]['l']=max(global_dev_pos[i][agg]['l'],dev_pos)
                            global_dev_neg[i][agg]['l']=min(global_dev_neg[i][agg]['l'],dev_neg)
                        #self.pc.add_local(f,oldKey,v,a,agg,'linear',theta_l)
                            pattern.append(self.addLocal(f[i],fval,v[i],agg,'linear',theta_l,describe,param,
                                                         dev_pos,dev_neg))
                    
            self.time['regression']+=time()-reg_start
            
        inner_loop_start=time()
 
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
#                     make_reference=time()
#                     temp=fd[oldIndex[i]:index].copy()
#                     self.time['make_reference']+=time()-make_reference
                    if not fd_valid[i] or not supp_valid[i]:
                        continue
                    n=index-oldIndex[i]
                    if n>=self.supp_l:
                        num_f[i]+=1
                        fval=tuple([getattr(oldKey,j) for j in f[i]])
                        f_dict[i][fval]=[oldIndex[i],index]
                        #fit(fd[oldIndex[i]:index],fval,i,n)
                    oldIndex[i]=index
                    
            oldKey=tup
            
        if oldKey:
            for i in range(size):
#                 make_reference=time()
#                 temp=fd[oldIndex[i]:].copy()
#                 self.time['make_reference']+=time()-make_reference
                if not fd_valid[i] or not supp_valid[i]:
                    continue
                n=oldKey.Index-oldIndex[i]+1
                if n>=self.supp_l:
                    num_f[i]+=1
                    fval=tuple([getattr(oldKey,j) for j in f[i]])
                    f_dict[i][fval]=[oldIndex[i]]
                    #fit(fd[oldIndex[i]:],fval,i,n)
        self.time['innerloop']+=time()-inner_loop_start
        

        for i in range(size):
            if len(f_dict[i])<self.supp_g:
                if self.supp_inf:#if toggle is on
                    self.failedf.add(tuple(f[i]))
                supp_valid[i]=False
            else:
                if self.sampling:
                    randlst=list(f_dict[i])
                    shuffle(randlst)
                    n_f=num_f[i]
                    X=1.96*1.96*0.5*0.5/(0.05*0.05)
                    samplesize=ceil(n_f*X/(n_f+X-1))
                    cur=0
                    for fval in randlst:
                        indices=f_dict[i][fval]
                        if len(indices)==2: #indices=[oldIndex,index]
                            fit(fd[indices[0]:indices[1]],fval,i,indices[1]-indices[0])
                        else: #indices=[oldIndex]
                            fit(fd[indices[0]:],fval,i,oldKey.Index-indices[0]+1)
                        cur+=1
                        if cur==samplesize:
                            for agg in valid_c_f[i]:
                                if valid_c_f[i][agg]/samplesize<self.lamb:
                                    sample_invalid[i][agg]={'c'}
                            
                            for agg in valid_l_f[i]:
                                if valid_l_f[i][agg]/samplesize<self.lamb:
                                    try:
                                        sample_invalid[i][agg].add('l')
                                    except KeyError:
                                        sample_invalid[i][agg]={'l'}
                else:
                    for fval in f_dict[i]:
                        indices=f_dict[i][fval]
                        if len(indices)==2: #indices=[oldIndex,index]
                            fit(fd[indices[0]:indices[1]],fval,i,indices[1]-indices[0])
                        else: #indices=[oldIndex]
                            fit(fd[indices[0]:],fval,i,oldKey.Index-indices[0]+1)
                    
        #sifting global            
        for i in range(size):
            if not fd_valid[i] or not supp_valid[i]:
                    continue
            for agg in valid_c_f[i]:
                if agg in sample_invalid[i] and 'c' in sample_invalid[i][agg]:
                    continue
                lamb_c=valid_c_f[i][agg]/num_f[i]
                if valid_c_f[i][agg]>=self.supp_g and lamb_c>self.lamb:
                    #self.pc.add_global(f,v,a,agg,'const',self.theta_c,lamb_c)
                    self.glob.append(self.addGlobal(f[i],v[i],agg,'const',self.theta_c,lamb_c,
                                                    global_dev_pos[i][agg]['c'],global_dev_neg[i][agg]['c']))
            
            for agg in valid_l_f[i]:
                if agg in sample_invalid[i] and 'l' in sample_invalid[i][agg]:
                    continue
                lamb_l=valid_l_f[i][agg]/num_f[i]
                if valid_l_f[i][agg]>=self.supp_g and lamb_l>self.lamb:
                    #self.pc.add_global(f,v,a,agg,'linear',str(self.theta_l),str(lamb_l))
                    self.glob.append(self.addGlobal(f[i],v[i],agg,'linear',self.theta_l,lamb_l,
                                                    global_dev_pos[i][agg]['l'],global_dev_neg[i][agg]['l']))
        
        if not self.fit:
            return 
        
        '''
        #adding local with f=empty set
        if not self.validateFd(group,0):
            return
        reg_start=time()
        describe=[mean(fd[agg]),mode(fd[agg]),percentile(fd[agg],25)
                                          ,percentile(fd[agg],50),percentile(fd[agg],75)]
                           
        #fitting constant
        theta_c=chisquare(fd[agg])[1]
        if theta_c>self.theta_c:
            pattern.append(self.addLocal([' '],[' '],group,a,agg,'const',theta_c,describe,'NULL'))
          
          
                    
        #fitting linear
        if  theta_c!=1 and ((self.reg_package=='sklearn' and all(attr in self.num for attr in group)
                            or
                            (self.reg_package=='statsmodels' and any(attr in self.num for attr in group)))):
            
            gl=list(group)
            if self.reg_package=='sklearn':
                lr=LinearRegression()
                lr.fit(fd[gl],fd[agg])
                theta_l=lr.score(fd[gl],fd[agg])
                n=len(fd)
                theta_l=1-(1-theta_l)*(n-1)/(n-len(group)-1)
                param=lr.coef_.tolist()
                param.append(lr.intercept_.tolist())
                param="'"+str(param)+"'"
            else: #statsmodels
                lr=sm.ols(agg+'~'+'+'.join(gl),data=fd,missing='drop').fit()
                theta_l=lr.rsquared_adj
                param=Json(dict(lr.params))
            
            if theta_l and theta_l>self.theta_l:
            #self.pc.add_local(f,oldKey,v,a,agg,'linear',theta_l)
                pattern.append(self.addLocal([' '],[' '],group,a,agg,'linear',theta_l,describe,param))
        self.time['regression']+=time()-reg_start
        '''
        
        if pattern:
            insert_start=time()
            self.conn.execute("INSERT INTO "+self.pattern_schema+"."+self.table+"_local values"+','.join(pattern))        
            self.time['insertion']+=time()-insert_start
    def fitmodel_with_division(self, fd, group, aggList, division): 
        #fd=d.sort_values(by=f).reset_index(drop=True)
        
        #check global support inference
        if group[:division] in self.failedf:
            return
        
        oldKey=None
        oldIndex=0
        num_f=0
        valid_l_f={}
        valid_c_f={}
        f=list(group[:division])
        v=list(group[division:])  
        sample_invalid={}
        global_dev_pos={}
        global_dev_neg={}
        for agg in aggList:
            global_dev_pos[agg]={}
            global_dev_pos[agg]['l']=float('-inf')
            global_dev_pos[agg]['c']=float('-inf')
            global_dev_neg[agg]={}
            global_dev_neg[agg]['l']=float('inf')
            global_dev_neg[agg]['c']=float('inf')
        #df:dataframe n:length    
        pattern=[]
        def fit(df,f,fval,v,n):
            if not self.fit:
                return
            reg_start=time()
            for agg in aggList:
                nonlocal global_dev_pos,global_dev_neg
                if agg not in sample_invalid or 'c' not in sample_invalid[agg]:
                    avg=mean(df[agg])
                    describe=[avg,mode(df[agg]),percentile(df[agg],25)
                                                  ,percentile(df[agg],50),percentile(df[agg],75)]
                    dev_pos=max(df[agg])-avg
                    dev_neg=min(df[agg])-avg                    
                    #fitting constant
                    theta_c=chisquare(df[agg].dropna())[1]
                    if theta_c>self.theta_c:
                        nonlocal valid_c_f
                        try:
                            valid_c_f[agg]+=1
                        except KeyError:
                            valid_c_f[agg]=1
                        global_dev_pos[agg]['c']=max(global_dev_pos[agg]['c'],dev_pos)
                        global_dev_neg[agg]['c']=min(global_dev_neg[agg]['c'],dev_neg)
                        #self.pc.add_local(f,oldKey,v,a,agg,'const',theta_c)
                        pattern.append(self.addLocal(f,fval,v,agg,'const',theta_c,describe,'NULL',
                                                     dev_pos,dev_neg))
                    
                #fitting linear
                if agg not in sample_invalid or 'l' not in sample_invalid[agg]:
                    if  theta_c!=1 and ((self.reg_package=='sklearn' and all(attr in self.num for attr in v)
                                        or
                                        (self.reg_package=='statsmodels' and all(attr in self.num for attr in v)))):
        
                        if self.reg_package=='sklearn': 
                            lr=LinearRegression()
                            lr.fit(df[v],df[agg])
                            theta_l=lr.score(df[v],df[agg])
                            theta_l=1-(1-theta_l)*(n-1)/(n-len(v)-1)
                            param=lr.coef_.tolist()
                            param.append(lr.intercept_.tolist())
                            param="'"+str(param)+"'"
                        else: #statsmodels
                            #num_cat=1
                            #for attr in v:
                                #if attr not in self.num:
                                    #num_cat*=self.group_rows[frozenset([attr])]
                            #if num_cat>500:
                                #return
                            if n<=len(v)+1:#negative R^2 for sure
                                return
                            lr=sm.ols(agg+'~'+'+'.join([attr if attr in self.num else 'C('+attr+')' for attr in v]),
                                      data=df,missing='drop').fit()
                            #theta_l=lr.rsquared_adj
                            theta_l=chisquare(df[agg],lr.predict())[1]
                            param=Json(dict(lr.params))
                            dev_pos=max(lr.resid)
                            dev_neg=min(lr.resid)
                            
                        if theta_l and theta_l>self.theta_l:
                            nonlocal valid_l_f
                            try:
                                valid_l_f[agg]+=1
                            except KeyError:
                                valid_l_f[agg]=1
                            global_dev_pos[agg]['l']=max(globa_dev_pos[agg]['l'],dev_pos)
                            global_dev_neg[agg]['l']=min(global_dev_neg[agg]['l'],dev_neg)
                        #self.pc.add_local(f,oldKey,v,a,agg,'linear',theta_l)
                            pattern.append(self.addLocal(f,fval,v,agg,'linear',theta_l,describe,param,
                                                         dev_pos,dev_neg))
                        
            self.time['regression']+=time()-reg_start
        
        inner_loop_start=time()
        change=False
        f_dict={}
        for tup in fd.itertuples():
            if oldKey:
                change=any([getattr(tup,attr)!=getattr(oldKey,attr) for attr in f])
            if change:
                index=tup.Index
#                 make_reference=time()
#                 temp=fd[oldIndex:index].copy()
#                 self.time['make_reference']+=time()-make_reference
                n=index-oldIndex
                if n>=self.supp_l:
                    num_f+=1
                    #fit(fd[oldIndex:index],f,v,n)
                    fval=tuple([getattr(oldKey,j) for j in f])
                    f_dict[fval]=[oldIndex,index]
                oldIndex=index
            oldKey=tup
            
        if oldKey:
#             make_reference=time()
#             temp=fd[oldIndex:].copy()
#             self.time['make_reference']+=time()-make_reference
            n=oldKey.Index-oldIndex+1
            if n>=self.supp_l:
                num_f+=1
                #fit(fd[oldIndex:],f,v,n)
                fval=tuple([getattr(oldKey,j) for j in f])
                f_dict[fval]=[oldIndex]
        self.time['innerloop']+=time()-inner_loop_start
        
        if len(f_dict)<self.supp_g:
            if self.supp_inf:#if toggle is on
                self.failedf.add(group[:division])
            return
        else:
            if self.sampling:
                randlst=list(f_dict)
                shuffle(randlst)
                n_f=num_f
                X=1.96*1.96*0.5*0.5/(0.05*0.05)
                samplesize=ceil(n_f*X/(n_f+X-1))
                cur=0
                for fval in randlst:
                    indices=f_dict[fval]
                    if len(indices)==2: #indices=[oldIndex,index]
                        fit(fd[indices[0]:indices[1]],f,fval,v,indices[1]-indices[0])
                    else: #indices=[oldIndex]
                        fit(fd[indices[0]:],f,fval,v,oldKey.Index-indices[0]+1)
                    cur+=1
                    if cur==samplesize:
                        for agg in valid_c_f:
                            if valid_c_f[agg]/samplesize<self.lamb:
                                sample_invalid[agg]={'c'}
                        for agg in valid_l_f:
                            if valid_l_f[agg]/samplesize<self.lamb:
                                try:
                                    sample_invalid[agg].add('l')
                                except KeyError:
                                    sample_invalid[agg]={'l'}
                                
            else:
                for fval in f_dict:
                    indices=f_dict[fval]
                    if len(indices)==2: #indices=[oldIndex,index]
                        fit(fd[indices[0]:indices[1]],f,fval,v,indices[1]-indices[0])
                    else: #indices=[oldIndex]
                        fit(fd[indices[0]:],f,fval,v,oldKey.Index-indices[0]+1)

        
        if pattern:
            insert_start=time()
            self.conn.execute("INSERT INTO "+self.pattern_schema+"."+self.table+"_local values"+','.join(pattern))
            self.time['insertion']+=time()-insert_start
        
        for agg in valid_c_f:
            if agg in sample_invalid and 'c' in sample_invalid[agg]:
                continue
            lamb_c=valid_c_f[agg]/num_f
            if valid_c_f[agg]>=self.supp_g and lamb_c>self.lamb:
                #self.pc.add_global(f,v,a,agg,'const',self.theta_c,lamb_c)
                self.glob.append(self.addGlobal(f,v,agg,'const',self.theta_c,lamb_c,
                                                global_dev_pos[agg]['c'],global_dev_neg[agg]['c']))
                
        for agg in valid_l_f:
            if agg in sample_invalid and 'l' in sample_invalid[agg]:
                continue
            lamb_l=valid_l_f[agg]/num_f
            if valid_l_f[agg]>=self.supp_g and lamb_l>self.lamb:
                #self.pc.add_global(f,v,a,agg,'linear',str(self.theta_l),str(lamb_l))
                self.glob.append(self.addGlobal(f,v,agg,'linear',self.theta_l,lamb_l,
                                                global_dev_pos[agg]['l'],global_dev_neg[agg]['l']))
                          
    def addLocal(self,f,f_val,v,agg,model,theta,describe,param,dev_pos,dev_neg):#left here
#         f="'"+str(f).replace("'","")+"'"
#         f_val="'"+str(f_val).replace("'","")+"'"
#         v="'"+str(v).replace("'","")+"'"
        f='ARRAY'+str(list(f)).replace('"','')
        f_val='ARRAY'+str([str(val).replace("'","") for val in f_val]).replace('"','')
        v='ARRAY'+str(list(v)).replace('"','')
        agg="'"+agg+"'"
        model="'"+model+"'"
        theta="'"+str(theta)+"'"
        describe="'"+str(describe).replace("'","")+"'"
        #return 'insert into '+self.table+'_local values('+','.join([f,f_val,v,a,agg,model,theta,describe,param])+');'
        return '('+','.join([f,f_val,v,agg,model,theta,describe,str(param),str(dev_pos),str(dev_neg)])+')'
    
    
    def addGlobal(self,f,v,agg,model,theta,lamb,dev_pos,dev_neg):
#         f="'"+str(f).replace("'","")+"'"
#         v="'"+str(v).replace("'","")+"'"
        f='ARRAY'+str(list(f)).replace('"','')
        v='ARRAY'+str(list(v)).replace('"','')
        agg="'"+agg+"'"
        model="'"+model+"'"
        theta="'"+str(theta)+"'"
        lamb="'"+str(lamb)+"'"
        #return 'insert into '+self.table+'_global values('+','.join([f,v,a,agg,model,theta,lamb])+');'
        return '('+','.join([f,v,agg,model,theta,lamb,str(dev_pos),str(dev_neg)])+')'
         
    
    def createTable(self,pattern_schema):
        if self.reg_package=='sklearn':
            type='varchar'
        else:
            type='json'
        
        self.conn.execute('CREATE SCHEMA IF NOT EXISTS '+pattern_schema)
        
        self.conn.execute('DROP TABLE IF EXISTS '+pattern_schema+'.'+self.table+'_local;')    
        self.conn.execute('create table IF NOT EXISTS '+pattern_schema+'.'+self.table+'_local('+
                     'fixed varchar[],'+
                     'fixed_value varchar[],'+
                     'variable varchar[],'+
                     'agg varchar,'+
                     'model varchar,'+
                     'theta float,'+
                     'stats varchar,'+
                     'param '+type+','+
                     'dev_pos float,'+
                     'dev_neg float);')
        
        self.conn.execute('DROP TABLE IF EXISTS '+pattern_schema+'.'+self.table+'_global')
        self.conn.execute('create table IF NOT EXISTS '+pattern_schema+'.'+self.table+'_global('+
                     'fixed varchar[],'+
                     'variable varchar[],'+
                     'agg varchar,'+
                     'model varchar,'+
                     'theta float,'+
                     'lambda float, '+
                     'dev_pos float,'+
                     'dev_neg float);')
        
        attr=''
        for key in self.time:
            attr+=key+' varchar,'
        self.conn.execute('create table IF NOT EXISTS time_naive('+
                          'id serial primary key,'+
                          attr+
                          'description varchar);')
        
        
    def insertTime(self,description):
        attributes=list(self.time)
        values=[str(self.time[i]) for i in attributes]
        attributes.append('description')
        values.append(description)
        self.conn.execute('INSERT INTO time_naive('+','.join(attributes)+') values('+','.join(values)+')')
        
    #below is for testing purpose
    def addTestGroup(self,fit_group,prefix,division):
        if division:
            if prefix[:division] not in fit_group:
                fit_group[prefix[:division]]=set()
            fit_group[prefix[:division]].add(prefix[division:])
        else:
            for div in range(1,len(prefix)):
                self.addTestGroup(fit_group, prefix, div)
