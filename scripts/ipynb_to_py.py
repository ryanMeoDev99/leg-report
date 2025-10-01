import nbformat

"""
Convert jupyter notebook into python file, 
separating each cell with a header and footer
    * header prefix: #====== [START] CELL {cell_number} ======
    * footer prefix: #====== [END] CELL {cell_number} ======

If it is markdown cell then,
    * markdown prefix: #====== [MARKDOWN] CELL {cell_number} ======
"""

def extract_code_from_ipynb(ipynb_file, output_file):
    with open(ipynb_file, 'r', encoding='utf-8') as file:
        notebook = nbformat.read(file, as_version=4)

    code_cells = notebook['cells']

    with open(output_file, 'w', encoding='utf-8') as file:
        for i, cell in enumerate(code_cells, 1):
            if cell['cell_type'] == 'code':
                file.write(f'#====== [START] CELL {i} ======\n\n')
                file.write(cell['source'])
                file.write(f'\n\n#====== [END] CELL {i} ======\n\n\n')
            if cell['cell_type'] == 'markdown':
                file.write(f'\n\n#====== [MARKDOWN] CELL {i} ======\n\n')


# Change the filename below to your corresponding input and output filename
dir = 'scripts'
input_file = f'{dir}/fct_total_revenue.ipynb'
output_file = f'{dir}/output.py'
extract_code_from_ipynb(input_file, output_file)
