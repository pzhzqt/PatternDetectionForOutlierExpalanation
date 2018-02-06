import sys
import sqlalchemy as sa
from PatternFinder import PatternFinder

def main():
    #pc=PC()
    config=input("Connection config (host dbname username password):\n").split()    
    try:
        engine = sa.create_engine(
                'postgresql://'+config[2]+':'+config[3]+'@'
                +config[0]+':5432/'+config[1],
                echo=True)
    except Exception as ex:
        print(ex)
        sys.exit(1)
    
    p=PatternFinder(engine.connect())
    p.findPattern()
    
    engine.dispose()
        
if __name__=="__main__":
    main()