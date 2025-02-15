import os
import shutil

def convert_sql_to_txt(root_dir):
    # Walk through all directories and subdirectories
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Find all .sql files
        for filename in filenames:
            if filename.endswith('.sql'):
                # Create the full file paths
                sql_file = os.path.join(dirpath, filename)
                txt_file = os.path.join(dirpath, filename[:-4] + '.txt')
                
                # Copy the .sql file to .txt
                # os.rename(sql_file, txt_file)
                
                os.remove(sql_file)
                print(f"Converted: {sql_file} -> {txt_file}")

if __name__ == "__main__":
    # Replace this with your folder path
    folder_path = "./models_txt"  # Current directory, change this to your specific path
    convert_sql_to_txt(folder_path)