import sys
import sqlalchemy as sa
from PatternFinder import PatternFinder

def main():
    #pc=PC()
    #config=input("Connection config (host port dbname username password):\n").split()
    config=['localhost','5432','postgres','antiprov','antiprov']
    try:
        engine = sa.create_engine(
                'postgresql://'+config[3]+':'+config[4]+'@'
                +config[0]+':'+config[1]+'/'+config[2],
                echo=False)
    except Exception as ex:
        print(ex)
        sys.exit(1)
    
    p=PatternFinder(engine.connect(),table='crime_exp_8',fit=True,reg_package='sklearn',
                    supp_l=15,supp_g=15,fd_check=False,supp_inf=False,algorithm='optimized')
    #fd=[(['A'],['B']),(['A','B'],['C'])]
    #p.addFd(fd)
    p.findPattern()
    engine.dispose()
        
if __name__=="__main__":
    main()
