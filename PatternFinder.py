import pandas as pd
from itertools import combinations
from Regression import fitlinear,fitconstant

class PatternFinder:
    conn=None
    table=None
    theta_c=None
    theta_l=None
    lamb=None
    cat=None
    num=None
    schema=None
    
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
                            f=[k for k in g if k not in v]
                            if all([x in num for x in v]):
                                fitlinear(d,f,v,agg)
                            fitconstant(d,f,v,agg)
    
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
        