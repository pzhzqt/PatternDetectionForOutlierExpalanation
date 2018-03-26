import sys
import sqlalchemy as sa
from PatternFinder import PatternFinder

def main():
    #pc=PC()
    #config=input("Connection config (host port dbname username password):\n").split()
    config=['debussy.cs.iit.edu','5436','antiprov','antiprov','antiprov']
    try:
        engine = sa.create_engine(
                'postgresql://'+config[3]+':'+config[4]+'@'
                +config[0]+':'+config[1]+'/'+config[2],
                echo=True)
    except Exception as ex:
        print(ex)
        sys.exit(1)
    
    p=PatternFinder(engine.connect(),'publication')
    p.findPattern()
    engine.dispose()
        
if __name__=="__main__":
    main()
