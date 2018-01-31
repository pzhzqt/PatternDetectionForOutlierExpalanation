import sys
import psycopg2
from sqlalchemy import create_engine
from PatternFinder import PatternFinder

def main():
    #pc=PC()
    config=input("Connection config (host dbname username password):\n").split()
    try:
        conn=psycopg2.connect(host=config[0],dbname=config[1],
                              user=config[2],password=config[3])
    except psycopg2.DatabaseError as ex:
        print(ex)
        sys.exit(1)
        
    try:
        engine = create_engine(
                'postgresql://'+config[2]+':'+config[3]+'@'
                +config[0]+':5432/'+config[1],
                echo=True)
    except Exception as ex:
        print(ex)
        sys.exit(1)
    
    p=PatternFinder(conn)
    p.findPattern()
    
    engine.dispose()
    conn.close()
if __name__=="__main__":
    main()