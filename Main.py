import sys
import psycopg2 as pg
from PatternFinder import PatternFinder

def main():
    #pc=PC()
    #config=input("Connection config (host port dbname username password):\n").split()
    #config=['216.47.152.61','5432','postgres','antiprov','test']
    try:
        #engine = sa.create_engine(
                #'postgresql://'+config[3]+':'+config[4]+'@'
                #+config[0]+':'+config[1]+'/'+config[2],
                #echo=True)
        conn=pg.connect("host='localhost' port= '5433' user='antiprov' password='antiprov' dbname='antiprov'")
    except Exception as ex:
        print(ex)
        sys.exit(1)

    p=PatternFinder(conn)
    p.findPattern()
    conn.close()
        
if __name__=="__main__":
    main()