import sqlalchemy as sa
import argparse
import pandas as pd
import random as rd

class Synthetic:
    pattern=None
    table=None
    pattern_schema=None
    conn=None
    schema=None
    num=None
    def __init__(self,tableName,pattern_table,connection,schema):
        self.table=tableName #table name
        self.pattern_table=pattern_table #schema for the pattern
        self.conn=connection #connection
        self.schema=schema #schema to store synthetic table
        self.num=0#track the number of tables generated
    
    def collectGlobal(self):
        self.pattern=None #initialize as none
        try:
            self.pattern=pd.read_sql('SELECT * FROM '+self.pattern_table,con=self.conn)
        except Exception as ex:
            print(ex)
    
    def synthesize(self,num,F_ratio,V_ratio):#randomly generate for num different patterns
        '''
            num: number of different tables to generate
            F_ratio: the ratio of F to modify for this pattern
            V_ratio: the ratio of V to modify for each F
        '''
        self.collectGlobal()
        if self.pattern.empty:
            print('No pattern')
            return
        self.conn.execute('CREATE SCHEMA IF NOT EXISTS '+self.schema)
        self.conn.execute('DROP TABLE IF EXISTS '+self.schema+'.'+self.table+'_changes')
        self.conn.execute('CREATE TABLE '+self.schema+'.'+self.table+'_changes('+
                          'num int,'
                          'F varchar[],'+
                          'fval varchar[],'+
                          'V varchar[],'+
                          'vold varchar[],'+
                          'vnew varchar[],'+
                          'agg varchar,'+
                          'Fratio float,'+
                          'Vratio float)')
        for pattern in self.pattern.sample(frac=1).reset_index(drop=True).itertuples():
            if self.num>=num:
                break
            #F=getattr(pattern,'fixed')[1:-1].split(',')
            #V=getattr(pattern,'variable')[1:-1].split(',')
            F=getattr(pattern,'fixed')
            V=getattr(pattern,'variable')
            agg=getattr(pattern,'agg')
            self.num+=1
            self.synthesize_pattern(F, V, agg, F_ratio, V_ratio)
        
    def synthesize_pattern(self,F,V,agg,F_ratio,V_ratio):#generate for a specific pattern
        fdom=pd.read_sql('select distinct '+','.join(F)+' from '+self.table,con=self.conn)
        fnum=int(len(fdom)*F_ratio) #total to be done
        vdom=pd.read_sql('select distinct '+','.join(V)+' from '+self.table,con=self.conn)
        self.conn.execute('DROP TABLE IF EXISTS '+self.schema+'.'+self.table+'_'+str(self.num))
        self.conn.execute('CREATE TABLE '+self.schema+'.'+self.table+'_'+str(self.num)+
                          ' AS SELECT * FROM '+self.table+
                          ' ORDER BY '+','.join(F))
        cur=0 #track how many already done
        for tup in fdom.sample(frac=1).reset_index(drop=True).itertuples():
            if cur>=fnum:
                break
            f=list(tup)[1:]
            v=vdom.sample(n=2).reset_index(drop=True)
            v_cur=list(v.iloc[0])#current v to change
            v_new=list(v.iloc[1])#target v to change to
            if len(V)==1:
                update=','.join(V)+'='+str(v_new)[1:-1]
            else:
                update='('+','.join(V)+')=('+str(v_new)[1:-1]+')'
            self.conn.execute('UPDATE '+self.schema+'.'+self.table+'_'+str(self.num)+
                              ' SET '+update+
                              ' WHERE ('+','.join(F)+')=('+str(f)[1:-1]+')'+
                              ' AND ('+','.join(V)+')=('+str(v_cur)[1:-1]+')'+
                              ' AND RANDOM()<'+str(V_ratio))
            self.conn.execute('INSERT INTO '+self.schema+'.'+self.table+'_changes values('+
                              str(self.num)+','+
                              'ARRAY'+str(list(F)).replace('"','')+','+
                              'ARRAY'+str([str(val).replace("'","") for val in f]).replace('"','')+','+
                              'ARRAY'+str(list(V)).replace('"','')+','+
                              'ARRAY'+str([str(val).replace("'","") for val in v_cur]).replace('"','')+','+
                              'ARRAY'+str([str(val).replace("'","") for val in v_new]).replace('"','')+','+
                              "'"+agg+"'"+','+
                              "'"+str(F_ratio)+"'"+','+
                              "'"+str(V_ratio)+"'"+')')
        
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("table", help="generate synthetic table for given table name")
    parser.add_argument("pattern_table", help="table where the pattern stored")
    parser.add_argument("--schema", help="schema to store synthetic data", default='synthetic')
    parser.add_argument("--num", help="number of different tables to generate", type=int, default=1)
    parser.add_argument("--Fratio", help="fixed attribute ratio to modify", type=float, default=0.03)
    parser.add_argument("--Vratio", help="variable attribute ratio to modify", type=float, default=0.5)
    args = parser.parse_args()
    config=['localhost','5436','antiprov','antiprov','antiprov']
    try:
        engine = sa.create_engine(
                'postgresql://'+config[3]+':'+config[4]+'@'
                +config[0]+':'+config[1]+'/'+config[2],
                echo=False)
    except Exception as ex:
        print(ex)
        sys.exit(1)
        
    synthetic=Synthetic(args.table,args.pattern_table,engine.connect(),args.schema)
    synthetic.synthesize(args.num, args.Fratio, args.Vratio)
if __name__=="__main__":
    main()
