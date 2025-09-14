import os
import subprocess
import shutil
from typing import List
import pandas as pd


def create_fasta_file(sequences: List[str], output_path: str):
    with open(output_path, 'w') as fasta_file:
        for idx, seq in enumerate(sequences):
            fasta_file.write(f">seq{idx}\n{seq}\n")

def evaluate_mmseqs_output(output_dir: str):
    
    # Read sequences from FASTA file
    sequences = [] 
    fasta_path = os.path.join(output_dir, "output_all_seqs.fasta")
    
    with open(fasta_path, 'r') as fasta_file:
        lines = fasta_file.readlines()
    
    i = 0
    cluster_id = None
    while i < len(lines) - 1:
        line1 = lines[i].strip()
        line2 = lines[i + 1].strip()
        
        # Check if we have a valid cluster_id-sequence pair
        if line1.startswith('>') and not line2.startswith('>'):
            sequence = line2
            sequences.append({'seq': sequence, 'cluster_id': cluster_id})
            i += 2  # Move to next pair
        else:
            cluster_id = line1[1:]  # Remove '>' character
            i += 1  # Skip this header and try next line

    df = pd.DataFrame(sequences)

    df['cluster_size'] = df.groupby('cluster_id')['cluster_id'].transform('count')
    return df

def run_mmseqs2(input_sequences: List[str], dir_name: str, threads=4, sensitivity=4) -> pd.DataFrame:
    # Create FASTA input file out of sequences
    os.makedirs(dir_name, exist_ok=True)
    create_fasta_file(input_sequences, os.path.join(dir_name, "input.fasta"))

    # Create tmp dir
    os.makedirs(os.path.join(dir_name, "tmp"), exist_ok=True)

    # Run MMseqs2 clustering (default settings)
    subprocess.run([
        "mmseqs",
        "easy-cluster",
        os.path.join(dir_name, "input.fasta"),
        os.path.join(dir_name, "output"),
        os.path.join(dir_name, "tmp"),
        "--min-seq-id", "0.5",
        "--threads", str(threads),
        "-s", str(sensitivity)
    ])

    # Remove tmp directory and all its contents
    tmp_dir = os.path.join(dir_name, "tmp")
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    # Evaluate output (placeholder)
    df = evaluate_mmseqs_output(os.path.join(dir_name))

    return df