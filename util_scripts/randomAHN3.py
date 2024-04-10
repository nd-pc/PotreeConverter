import os
import json
import subprocess

header_dir = '/scratch/anauman/escience/projects/nD-PC/ahn3_headers'
output_dir = '/home/anauman/staff-umbrella/ahn3_sim_data'
num_points = 2000000

# Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


# Get a list of all header JSON files in the directory
header_files = os.listdir(header_dir)

# Process each header file
for header_file in header_files:
    # Read the header JSON file
    with open(os.path.join(header_dir, header_file)) as f:
        header_data = json.load(f)

    # Extract the min and max bounds
    minx = header_data['metadata']['minx']
    miny = header_data['metadata']['miny']
    minz = header_data['metadata']['minz']
    maxx = header_data['metadata']['maxx']
    maxy = header_data['metadata']['maxy']
    maxz = header_data['metadata']['maxz']

    # Generate a random LAZ file using the bounds
    output_file = os.path.splitext(header_file)[0] + '.LAZ'
    output_path = os.path.join(output_dir, output_file)
    subprocess.run(['pdal', 'random',
                     '--bounds', f'([{minx},{maxx}],[{miny},{maxy}],[{minz},{maxz}])',
                     '--count', str(num_points),
                     '--output', output_path
                     , "--distribution", "uniform"])

    print(f"Generated random LAZ file: {output_file}")
