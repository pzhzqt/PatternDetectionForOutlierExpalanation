import sys
import psycopg2
import PatternStore
import Clustering
import RegressionGeneralized as reg
from sqlalchemy import create_engine
from matplotlib.backends.backend_pdf import PdfPages


engine = None
conn = None
pdf = PdfPages('PatternsStock.pdf')

class PatternFinder:
    categories = None
    dimensions = None
    values = None
    patternList = []
    cursor = None
    data = None

    def __init__(self, time, categories, dimensions, values, data):
        try:
            global conn
            conn = psycopg2.connect(dbname='postgres', user='postgres',
                                    host='localhost', password='postgres')
        except psycopg2.DatabaseError as ex:
            print(ex)
            sys.exit(1)

        try:
            global engine
            engine = create_engine(
                'postgresql://postgres:postgres@localhost:5432/postgres',
                echo=True)
        except Exception as ex:
            print(ex)
            sys.exit(1)

        self.cursor = conn.cursor()
        self.data = data
        self.categories = categories

        #for testing
        # self.values = values
        # self.dimensions = dimensions+time
        #for testing
        

        #org
        global conn
        reduced_dimensions, reduced_values = Clustering.Cluster(dimensions,
                                                                values,
                                                                self.data,
                                                                conn)
        print(reduced_dimensions)
        print(reduced_values)
        
        
        self.dimensions = reduced_dimensions + time
        self.values = reduced_values
        #org


        PatternStore.create_table_object(self.data)
        self.formDatacube()

    def findPatterns(self):
        
        print('Dimension:: ', self.dimensions)
        #categories as fixed
        for f in self.categories:
            
            #categories in variable
            for v in self.categories:
                    self.patternList.append(self.findConstant(f, v, val)
                                            for val in self.values)

            #dimensions in variable
            for v in self.dimensions:
                for val in self.values:
                    self.findRegressions(f, v, "avg", val)
                    
                                    
                '''
                self.patternList.append(self.findRegressions(f, v, val)
                                        for val in self.values)
                self.patternList.append(self.findConstants(f, v, val)
                                        for val in self.values)
                '''

        #dimensions as fixed
        for f in self.dimensions:
            
            # categories in variable
            for v in self.categories:
                self.patternList.append(self.findConstant(f, v, val)
                                        for val in self.values)

            # dimensions in variable
            for v in self.dimensions:
                self.patternList.append(self.findRegressions(f, v, val)
                                        for val in self.values)
                self.patternList.append(self.findConstant(f, v, val)
                                        for val in self.values)

        #close all database connections
        global engine
        engine.dispose()
        conn.close()
        pdf.close()


    def findRegressions(self, fixed, variable, aggFunc, value):
        query = reg.formQuery(fixed, variable, aggFunc, value, self.data)
        self.cursor.execute(query)
                
        dictFixed = {}
        reg.formDictionary(self.cursor, dictFixed)
        reg.fitRegressionModel(dictFixed, fixed, variable, aggFunc, value)
        return []

    def findConstants(self, fixed, variable, value):
        return []

    def formDatacube(self):
        values_avg_cols = [s+"_avg" for s in self.values]
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
        avg = ",".join(" avg("+v+") " for v in self.values)
        std = ",".join(" stddev_pop("+v+") " for v in self.values)
        query_insert = "INSERT INTO "+self.data+"_datacube ( "+insert+\
                       " ) SELECT "+select+","+avg+","+std+" FROM "+self.data+\
                       " GROUP BY CUBE ( "+select+" );"
        print(query_insert)
        # self.cursor.execute(query_insert)


        
