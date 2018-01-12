
import sys
import psycopg2
import PatternStore
import Clustering
import RegressionGeneralized as reg
from sqlalchemy import create_engine
#from matplotlib.backends.backend_pdf import PdfPages


engine = None
conn = None
#pdf = PdfPages('PatternsStock.pdf')

class PatternFinder:
    categories = None
    dimensions = None
    values = None
    cursor = None
    data = None

    def __init__(self, time, categories, dimensions, values, data):
        try:
            global conn
            conn = psycopg2.connect(dbname='postgres', user='antiprov',
                                    host='127.0.0.1', password='test')
        except psycopg2.DatabaseError as ex:
            print(ex)
            sys.exit(1)

        try:
            global engine
            engine = create_engine(
                'postgresql://antiprov:test@127.0.0.1:5432/postgres',
                echo=True)
        except Exception as ex:
            print(ex)
            sys.exit(1)

        
        self.cursor = conn.cursor()
        self.data = data
        self.categories = categories

        #for testing
        self.values = values
        self.dimensions = dimensions+time
        #for testing

    
        '''
        #org begin
        global conn
        reduced_dimensions, reduced_values = Clustering.Cluster(dimensions,
                                                                values,
                                                                self.data,
                                                                conn)
        print(reduced_dimensions)
        print(reduced_values)
        
        
        self.dimensions = reduced_dimensions + time
        self.values = reduced_values
        #org end
        '''
        
        PatternStore.create_table_object(self.data)
        self.formDatacube()

    def findPatterns(self):
        dimension_subsets = self.get_subsets(self.dimensions)
        others = self.categories + self.dimensions
        others_subsets = self.get_subsets(others)

        #Regression combinations
        for f in others_subsets:
            for v in dimension_subsets:
                if len(set(f) & set(v)) == 0:
                    for val in self.values:
                        self.findRegressions(f, v, "sum", val)

        #Constant combinations:
        for f in others_subsets:
            for v in others_subsets:
                if len(set(f) & set(v)) == 0:
                    for val in self.values:
                        self.findConstants(f, v, val)

        # for f in self.categories:
        #     #categories in variable
        #     for val in self.values:
        #         self.findConstants2(f, val)
        #
        # print('Dimension:: ', self.dimensions)
        # #categories as fixed
        # for f in self.categories:
        #
        #     #categories in variable
        #     for v in self.categories:
        #             self.patternList.append(self.findConstant(f, v, val)
        #                                     for val in self.values)
        #
        #     #dimensions in variable
        #     '''
        #     for v in self.dimensions:
        #         for val in self.values:
        #             self.findRegressions(f, v, "avg",val)
        #     '''
        #
        #     v = ['month', 'day']
        #     f = ['ticker', 'year']
        #     for val in self.values:
        #         self.findRegressions(f, v, "avg", val)
        #         self.findConstants(f, v, val)

                

                    
                                                        
            '''
            self.patternList.append(self.findRegressions(f, v, val)
                                        for val in self.values)
            self.patternList.append(self.findConstants(f, v, val)
                                        for val in self.values)
            '''

        #dimensions as fixed
        # for f in self.dimensions:
        #
        #     # categories in variable
        #     for v in self.categories:
        #         self.patternList.append(self.findConstant(f, v, val)
        #                                 for val in self.values)
        #
        #     # dimensions in variable
        #     for v in self.dimensions:
        #         self.patternList.append(self.findRegressions(f, v, val)
        #                                 for val in self.values)
        #         self.patternList.append(self.findConstant(f, v, val)
        #                                 for val in self.values)

        #close all database connections
        global engine
        engine.dispose()
        conn.close()
        #pdf.close()

    def get_subsets(self, l):
        n = len(l)
        subsets = []
        temp = []
        for i in range(0, pow(2, n)):
            count = 0
            for j in range(0, n):
                if (i & (1 << j)) > 0:
                    temp.append(l[j])
                    count = count + 1

                if count == 4:
                    break

            if len(temp) > 0:
                subsets.append(temp)
                temp = []

        return subsets

    def findRegressions(self, fixed, variable, aggFunc, value):
        query = reg.formQuery(fixed, variable, aggFunc, value, self.data)
        self.cursor.execute(query)
                
        dictFixed = {}
        reg.formDictionary(self.cursor, dictFixed, fixed, variable)
        reg.fitRegressionModel(dictFixed, fixed, variable, aggFunc, value)
        return []


    def findConstants(self, fixed, variable, value):
        query = Clustering.formQuery(fixed, variable, value, self.data)
        self.cursor.execute(query)
        print(query)
        dictFixed = {}
        reg.formDictionary(self.cursor, dictFixed, fixed, variable)
        Clustering.findConstants(dictFixed, fixed, variable, value)
        return []

    def formDatacube(self):
        values_avg_cols = [s+"_sum" for s in self.values]
        values_std_cols = [s+"_std" for s in self.values]

        categories_str = ' text, '.join(c for c in self.categories)
        categories_str = categories_str+" text" if len(self.categories) > 0 \
            else categories_str

        dimensions_str = ' decimal, '.join(d for d in self.dimensions)
        dimensions_str = dimensions_str+" decimal" if len(self.dimensions) > 0\
            else dimensions_str

        values_avg = ' decimal, '.join(v for v in values_avg_cols)
        values_avg = values_avg + " decimal" if len(values_avg_cols) > 0 \
            else values_avg

        values_std = ' decimal, '.join(v for v in values_std_cols)
        values_std = values_std + " decimal" if len(values_std_cols) > 0 \
            else values_std

        #Create table for datacube
        query_create_table = "CREATE TABLE "+self.data+"_datacube(" +\
                             categories_str+"," +\
                             dimensions_str+", " +\
                             values_avg+", " +\
                             values_std+" );"
        print(query_create_table)
        # self.cursor.execute(query_create_table)

        #Insert into datacube table
        insert_list = self.categories + self.dimensions + \
                      values_avg_cols + values_std_cols
        select_list = self.categories+self.dimensions
        insert = ",".join(s for s in insert_list)
        select = ",".join(s for s in select_list)
        avg = ",".join(" sum("+v+") " for v in self.values)
        std = ",".join(" stddev_pop("+v+") " for v in self.values)
        query_insert = "INSERT INTO "+self.data+"_datacube ( "+insert+\
                       " ) SELECT "+select+","+avg+","+std+" FROM "+self.data+\
                       " GROUP BY CUBE ( "+select+" );"
        print(query_insert)
        # self.cursor.execute(query_insert)
    
'''       
    def findConstants2(self, fixed, value):
        query = Clustering.formQuery2(fixed, value, self.data)
        self.cursor.execute(query)                
        dictFixed = {}
        Clustering.formDictionary2(self.cursor, dictFixed)
        Clustering.findConstants2(dictFixed, fixed, value)
        return []
'''
