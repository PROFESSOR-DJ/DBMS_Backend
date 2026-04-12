# input_path  = r'C:\Users\DHIRAAJ K V\.Neo4jDesktop2\Data\dbmss\dbms-044a5463-53e9-4d56-a581-92ef540ee4ff\import\papers.csv'
# output_path = r'C:\Users\DHIRAAJ K V\.Neo4jDesktop2\Data\dbmss\dbms-044a5463-53e9-4d56-a581-92ef540ee4ff\import\papers_fixed.csv'

# import re

# fixed_lines = []
# skipped = 0

# with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
#     lines = f.readlines()

# header = lines[0]
# fixed_lines.append(header)

# for i, line in enumerate(lines[1:], start=2):
#     # Fix the specific pattern: character(s) appearing after a closing quote before comma or newline
#     # "some text""more text" -> "some textmore text"
#     # This regex finds "" that are NOT at the start of a field (not a legitimate escape)
    
#     # Remove the problematic double-quotes that appear after word characters
#     fixed = re.sub(r'([a-zA-Z0-9\s])""([a-zA-Z])', r'\1\2', line)
    
#     # Also collapse any 3+ consecutive quotes to double (legitimate escape)
#     fixed = re.sub(r'"{3,}', '""', fixed)
    
#     fixed_lines.append(fixed)

# with open(output_path, 'w', encoding='utf-8') as f:
#     f.writelines(fixed_lines)

# print(f"Done. {len(fixed_lines)-1:,} rows written, {skipped} skipped")

# # Verify
# import csv
# with open(output_path, 'r', encoding='utf-8') as f:
#     reader = csv.reader(f)
#     try:
#         rows = sum(1 for _ in reader)
#         print(f"Verified: {rows:,} rows parse cleanly")
#     except Exception as e:
#         print(f"Error: {e}")


# # import csv
# # with open(r'C:\Users\DHIRAAJ K V\.Neo4jDesktop2\Data\dbmss\dbms-044a5463-53e9-4d56-a581-92ef540ee4ff\import\papers_clean.csv', encoding='utf-8') as f:
# #     reader = csv.reader(f)
# #     for i, row in enumerate(reader):
# #         pass
# #     print(f'All {i+1} rows parsed cleanly')


import re

f = r'C:\Users\DHIRAAJ K V\.Neo4jDesktop2\Data\dbmss\dbms-044a5463-53e9-4d56-a581-92ef540ee4ff\import\papers.csv'

with open(f, 'r', encoding='utf-8', errors='replace') as file:
    content = file.read()

original_size = len(content)

# Remove ALL occurrences of \"\" that appear between or adjacent to letters/digits
# These are ALL broken special characters (e, u, o with accents etc)
fixed = re.sub(r'\"\"', '', content)

with open(f, 'w', encoding='utf-8') as file:
    file.write(fixed)

removed = original_size - len(fixed)
print(f'Done. Removed {removed} bad characters across entire file.')
