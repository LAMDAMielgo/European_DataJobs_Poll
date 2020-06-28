import argparse
from p_acquisition import m_acquisition as m_ac

def greet():
    ## Dejo esto porque me sirve para saber donde ando
    print('Hello World!')


########################################## pendiente utilizar argparse
def argument_parser():
    parser = argparse.ArgumentParser(description = 'Set chart type')

    # argumentos a pasar
    parser.add_argument('-n', '--nombre', type = str, help = 'specify a name...')
    parser.add_argument('-c_n', '--country_name', type = str, help= 'Specify an country or [All] ...')
    args = parser.parse_args()
    return args

def main(arguments):
    print(f' print arf_nombre: {arguments.nombre}')
    
##########################################

if __name__ == '__main__':
    """
    year = int(input('Enter the year: '))
    title = 'Top 10 Manufacturers by Fuel Efficiency ' + str(year)
    arguments = argument_parser()
    main(arguments)
    """
    acquire_raw_data()