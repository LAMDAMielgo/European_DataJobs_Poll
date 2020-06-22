import argparse
# from p_acquisition import m_acquisition as m_ac

def argument_parser():
    parser = argparse.ArgumentParser(description = 'Set chart type')
    parser.add_argument('-n', '--nombre', type = str, help = 'specify a name...')
    args = parser.parse_args()
    return args

def main(arguments):
    print(f' print arf_nombre: {arguments.nombre}')
    

if __name__ == '__main__':
    """year = int(input('Enter the year: '))
    title = 'Top 10 Manufacturers by Fuel Efficiency ' + str(year) """
    arguments = argument_parser()
    main(arguments)