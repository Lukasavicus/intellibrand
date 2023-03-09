import json
import pandas as pd
from zipfile import ZipFile
import argparse

def read_data_from_zip(filename, limit=None):
    """
        This function read a Zip File and extract all data from json files inside of it.
        --------
        params:
            filename: complete path to Zip file
            limit: integer number of json files to be readed. If None passed read all json files from zip.
    """
    all_data = []
    errors = []
    
    with ZipFile(filename) as myzip:
        zip_info = myzip.infolist()
        for i, zipfile in enumerate(zip_info):
            # print("READING", zipfile.filename)
            if('.json' not in zipfile.filename):
                continue
            
            if(limit is not None):
                limit -= 1
            
            if(limit == 0):
                break
            
            try:
                with myzip.open(zipfile.filename) as myfile:
                    data = json.load(myfile)
                    all_data.append({
                            'file_properties' :
                            {
                                'title' : zipfile.filename
                            },
                            'content' : data,
                            'sk' : i
                        })
            except Exception as e:
                print(e)
                errors.append({'file' : zipfile.filename, 'error' : e, 'sk' : i})
                raise e
    # print("Readed", len(all_data))
    # only_data = [d['content'] for d in all_data]

    return all_data

def flatten_assortment(registry):
    """
        Function to flatten the assortment registry, based upon the hypothesis that the passed registry is an assortment registry.
        Extract all data from an assortment registry.
        -------
        params:
            registry: Assortment registry.
    """
    fprops = registry['file_properties']
    content = registry['content']
    assortments = content['assortment']
    flatten_registries = []
    for assortment in assortments:
#         print('assortment', assortment)
#         print('fprops', fprops)
        assrtmnt = {**fprops, **assortment}
        assrtmnt['sk'] = registry['sk']
        flatten_registries.append(assrtmnt)
    return flatten_registries

def transform_data(input, output):
    """
        Function that read a zip file containing all jsons files and transform all these data into a single parquet file.
    """
    all_data = read_data_from_zip(input) # 'datatest.zip'
    # print('first data', all_data[0])
    

    flattened_data = [flatten_assortment(d) for d in all_data]
    flattened_list = [item for sublist in flattened_data for item in sublist]
    df = pd.DataFrame(flattened_list)
    
    df.reset_index(drop=True, inplace=True)
    
    output += ".parquet.gzip"
    df.to_parquet(output, compression='gzip') # 'datatest-script-generated.parquet.gzip'

def cmd():
    """
        Create a CLI to help to process data
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", required=True, help="Input filename")
    ap.add_argument("-o", "--output", required=True, help="Output filename")
    args = ap.parse_args()

    return args

def main():
    """
        Main function, invoke cmd and process arguments to transform_data function.
    """
    args = cmd()
    transform_data(args.file, args.output)


if(__name__ == '__main__'):
    main()
    # USAGE: python .\script.py -f "datatest.zip" -o "dataset"