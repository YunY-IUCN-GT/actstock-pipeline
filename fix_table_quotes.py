#!/usr/bin/env python3
"""
Quick script to quote all numbered table names in SQL queries
"""
import re
import glob
import os

# Pattern to find unquoted numbered tables
pattern = r'\b(FROM|JOIN|INSERT INTO|UPDATE|DELETE FROM)\s+(0[0-9]_[a-z_]+)'

def fix_file(filepath):
    """Add quotes around numbered table names in a file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Replace unquoted table names with quoted ones
    def quote_table(match):
        keyword = match.group(1)
        table = match.group(2)
        return f'{keyword} "{table}"'
    
    content = re.sub(pattern, quote_table, content)
    
    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

# Find all Python and SQL files
search_dirs = ['batch', 'api', 'airflow/dags', 'collector', 'database']
fixed_count = 0

for search_dir in search_dirs:
    dir_path = os.path.join(os.path.dirname(__file__), search_dir)
    if not os.path.exists(dir_path):
        continue
    
    for ext in ['*.py', '*.sql']:
        for filepath in glob.glob(os.path.join(dir_path, '**', ext), recursive=True):
            if fix_file(filepath):
                print(f'Fixed: {filepath}')
                fixed_count += 1

print(f'\nTotal files fixed: {fixed_count}')
