import pandas as pd
from typing import List
import time

def sample_allocation(df: pd.DataFrame, group_keys: List[str], n_samples: int, return_allocation_only: bool = False):
    """
    Allocate and optionally sample as uniformly as possible across groups using simple round-robin.
    
    Args:
        df: DataFrame to sample from
        group_keys: Column names to group by (can be single string or list)
        n_samples: Total number of samples to allocate/select
        return_allocation_only: If True, return only allocation dict. If False, return sampled indices.

    Returns:
        If return_allocation_only=True: dict mapping group values to number of samples allocated
        If return_allocation_only=False: list of selected sample indices
    """
    if n_samples > len(df):
        raise ValueError("n_samples exceeds total number of available samples.")
    
    # Handle single group key
    if isinstance(group_keys, str):
        group_keys = [group_keys]
    
    # Get group information
    groups = df.groupby(group_keys)
    group_names = list(groups.groups.keys())

    print(f"Number of groups: {len(group_names)}")
    
    # Get max possible samples per group
    max_samples_per_group = {}
    for group_name in group_names:
        if len(group_keys) == 1:
            group_data = groups.get_group((group_name,))
        else:
            group_data = groups.get_group(group_name)
        max_samples_per_group[group_name] = len(group_data)
    
    # Initialize allocation
    samples_per_group = {group_name: 0 for group_name in group_names}
    
    # Round-robin allocation
    remaining = n_samples
    while remaining > 0:
        progress_made = False
        
        for group_name in group_names:
            if remaining <= 0:
                break
            
            # Add one sample if group can take it
            if samples_per_group[group_name] < max_samples_per_group[group_name]:
                samples_per_group[group_name] += 1
                remaining -= 1
                progress_made = True
        
        # Break if no group can take more samples
        if not progress_made:
            break
    
    # Return allocation only if requested
    if return_allocation_only:
        return samples_per_group
    
    # Actually sample from each group
    selected_indices = []
    for group_name, n_from_group in samples_per_group.items():
        if n_from_group > 0:
            if len(group_keys) == 1:
                group_data = groups.get_group((group_name,))
            else:
                group_data = groups.get_group(group_name)
            sampled = group_data.sample(n=n_from_group, random_state=42)
            selected_indices.extend(sampled.index.tolist())
    
    return selected_indices