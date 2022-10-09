import os
import csv

# Print Options
definiton_data_folder = "/tmp/data/"
definiton_q_folder = "/tmp/queries/"


def print_menu():
    print("[1] Generate the data.")
    print("[2] Create schemas and load data.")
    print("[3] Run queries.")
    print("[4] Exit.")

# Definitons of functions
def o1_generatedata():
    print(" ")
    print("Generate the data")
    scale = float(input('[PARAMS] Enter the scale factor: '))
    
    print(" ")
    os.mkdir("/tmp/data")
    os.mkdir("/tmp/queries")
    os.chdir("/tpcds-kit/tools")
    os.system("./dsdgen -SCALE "+ str(scale) + " -DIR "+definiton_data_folder)
    # DELIMITER =  <s>         -- use <s> as output field separator |
    # SUFFIX =  <s>            -- use <s> as output file suffix
    # TERMINATE =  [Y|N]       -- end each record with a field delimiter |
    # FORCE =  [Y|N]           -- over-write data files without prompting
    print(" ")
    print("Complete: Data generation")


def o1_generatequeries():
    print(" ")
    print("Generate the queries")
    scale = float(input('[PARAMS] Enter the scale factor: '))

    print(" ")
    os.chdir("/tpcds-kit/tools")
    os.system("./dsqgen -DIRECTORY ../query_templates -INPUT ../query_templates/templates.lst -VERBOSE Y -QUALIFY Y -SCALE "+ str(scale)+ "-DIALECT ../query_templates/sparksql -OUTPUT_DIR "+definiton_q_folder)
    # DELIMITER =  <s>         -- use <s> as output field separator |
    # SUFFIX =  <s>            -- use <s> as output file suffix
    # TERMINATE =  [Y|N]       -- end each record with a field delimiter |
    # FORCE =  [Y|N]           -- over-write data files without prompting
    print(" ")
    print(os.system("ls " + definiton_data_folder))
    os.chdir("/home/")
    print("Complete: Queries generation")


def o2_createschemas():
    print(" ")
    print("Create schemas")
    print(os.chdir("/home/"))
    os.system("python3 import_data.py")
    print("Finished creating schemas and importing the data.")


def o3_generatedata():
    print(" ")
    print("Run queries")


while(True):
    print_menu()
    try:
        option = int(input('Enter your choice: '))
    except:
        print("ERROR --- Incorrect input.")
        continue
    #Check the selection
    if option == 1:
        o1_generatedata()
    elif option == 2:
        o2_createschemas()
    elif option == 3:
        o1_generatequeries()
    elif option == 4:
        print('Exit')
        exit()
    else:
        print('Invalid option. Please enter a number between 1 and 4.')