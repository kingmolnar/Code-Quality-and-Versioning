#!/usr/bin/env python3
import sys
import os
import spacy
nlp = spacy.load('en_core_web_sm')

from booknlp.booknlp import BookNLP

DATA_DIR = "data/complaints_employment/text"
OUTPUT_DIR = "output/complaints_employment"

def file_list(datdir):
    for lin in os.popen(f"ls {datdir}/*.txt").readlines():
        yield os.path.abspath(lin.strip())
        

def main(input_directory, output_directory, verbose=True):
    model_params={
        "pipeline":"entity,quote,supersense,event,coref", 
        "model":"big"
    }

    booknlp=BookNLP("en", model_params)

    for i, input_file in enumerate(file_list(input_directory)):
        # File within this directory will be named ${book_id}.entities, ${book_id}.tokens, etc.
        if verbose:
            print(f"\nDoc: {i:5d}   File: {input_file}")
        book_id = os.path.basename(input_file).replace('.txt', '')
        try:
            booknlp.process(input_file, output_directory, book_id)
        except Exception as ex:
            print(f"FAILED: {input_file}")
            print(f"ERROR: {ex}")
            

    print("\nDone.")


if __name__=='__main__':
    main(DATA_DIR, OUTPUT_DIR)
    