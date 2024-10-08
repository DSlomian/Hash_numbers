import pandas as pd
import time
import psutil
import logging


logging.basicConfig(filename='program_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_and_map_id(filename, id_columns):
    df = pd.read_csv(filename, sep=' ', header=None)
    id_values = df.iloc[:, id_columns].astype(str).apply(lambda row: ' '.join(row), axis=1)
    return id_values

def process_files(file1_name, file2_name, file3_name):
    start_time = time.time()  

    file1 = read_and_map_id(file1_name, [0])
    file2 = read_and_map_id(file2_name, [0])
    file2_1 = read_and_map_id(file2_name, [1])
    file2_2 = read_and_map_id(file2_name, [2])
    file3 = read_and_map_id(file3_name, [0])

    all_ids = pd.concat([file1, file2, file2_1, file2_2, file3], ignore_index=True).unique()

    all_ids_filtered = [id_ for id_ in all_ids if len(str(id_)) > 4]

    id_mapping = {id_: i + 1 for i, id_ in enumerate(all_ids_filtered)}

    id_mapping_df = pd.DataFrame(list(id_mapping.items()), columns=['ID', 'Numer'])
    id_mapping_df.to_csv(f'IDs_hasnumbers.txt', index=False, sep=' ')

    def update_file(filename, id_columns, id_mapping):
        df = pd.read_csv(filename, sep=' ', header=None, dtype={col: str for col in id_columns})

        for col in id_columns:
            df[col] = df[col].map(id_mapping).fillna(df[col])

        return df

    file1_updated = update_file(file1_name, [0], id_mapping)
    file2_updated = update_file(file2_name, [0, 1, 2], id_mapping)
    file3_updated = update_file(file3_name, [0], id_mapping)

    file1_updated.to_csv(f'{file1_name.split(".")[0]}_hashnumbers.txt', index=False, header=False, sep=' ')
    file2_updated[[1, 2]] = file2_updated[[1, 2]].astype(int)
    file2_updated.to_csv(f'{file2_name.split(".")[0]}_hashnumbers.txt', index=False, header=False, sep=' ')
    file3_updated.to_csv(f'{file3_name.split(".")[0]}_hashnumbers.txt', index=False, header=False, sep=' ')

    end_time = time.time()  

   
    elapsed_time = end_time - start_time
    memory_usage = psutil.Process().memory_info().rss / 1024 / 1024  
    logging.info(f"Program executed successfully in {elapsed_time:.2f} seconds.")
    logging.info(f"Memory usage: {memory_usage:.2f} MB.")


file1_name = input("Enter the name of phenotype file: ")
file2_name = input("Enter the name of pedigree file: ")
file3_name = input("Enter the name of genotype file : ")


process_files(file1_name, file2_name, file3_name)

