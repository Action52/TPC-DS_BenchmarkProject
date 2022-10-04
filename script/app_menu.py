import os
import csv

# Print Options
definiton_data_folder="/home/TDC-DS_BenchmarkProject/data"

def print_menu():
    print("[1] Generate the data.")
    print("[2] Create schemas and load data.")
    print("[3] Run queries.")
    print("[4] Exit.")

# Definitons of functions
def o1_generatedata():
    print(" ")
    print("Generate the data")
    scale=1
    while(True):
        try:
            scale = float(input('[PARAMS] Enter the scale factor: '))
            break
        except:
            print("ERROR --- Incorrect input.")
            continue
    
    print(" ")
    os.chdir("/tpcds-kit/tools")
    os.system("./dsdgen -scale "+ str(scale)+ "-dir "+definiton_data_folder)
    # DELIMITER =  <s>         -- use <s> as output field separator |
    # SUFFIX =  <s>            -- use <s> as output file suffix
    # TERMINATE =  [Y|N]       -- end each record with a field delimiter |
    # FORCE =  [Y|N]           -- over-write data files without prompting
    os.system("./dsdgen -scale "+ str(scale)+ "-dir "+definiton_data_folder+" -sufffix .csv")
    print(" ")
    print("Complete: Data generation")
    

    
def convert_dattocsv():
    os.chdir(definiton_data_folder)
    os.system("mkdir data_csv")
    base_folder_csv="/tmp/data_csv/"

    dir_path ="/tmp"

    print("Running...")
    # Iterate directory
    for path in os.listdir(dir_path):
        # check if current path is a file
        if os.path.isfile(os.path.join(dir_path, path)):
            name=path.split(".")[0]
            print("Processing: "+ path)
            if(path.endswith(".dat")):
                with open(path) as dat_file, open(base_folder_csv+name+'.csv', 'w') as csv_file:
                    csv_writer = csv.writer(csv_file)

                    for line in dat_file:
                        row = [field.strip() for field in line.split('|')]
                        csv_writer.writerow(row)
                print("Complete: " + path)


def o2_createschemas():
    print(" ")
    print("Create schemas")

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
        o3_generatedata()
    elif option == 4:
        print('Exit')
        exit()
    else:
        print('Invalid option. Please enter a number between 1 and 4.')