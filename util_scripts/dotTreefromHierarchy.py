import struct
import sys
from pathlib import Path
import pprint
from io import BytesIO
from tqdm import tqdm

# Define the enum for node types
TYPE = {
    0: "NORMAL",
    1: "LEAF",
    2: "PROXY",
}

def print_nodes(file_path, start, size):
    with open(file_path, "rb") as binary_file:
        binary_file.seek(start)
        bytes_read = 0
        points = 0
        while bytes_read < size:
            data = binary_file.read(22)
            bytes_read += 22
            if not data:
                break
            (node_type, child_mask, numPoints, byteOffset, byteSize) = struct.unpack("<BBIQQ", data)
            if(node_type != 2):
                points += numPoints
                #if byteOffset + byteSize > 13460174264:
                 #    print("Error: byteOffset + byteSize > 849367283736" + " byteOffset: " + str(byteOffset) + " byteSize: " + str(byteSize))
                  #   exit(1)
                #print("node_type", TYPE[node_type], "child_mask", bin(child_mask), "numPoints", numPoints, "byteOffset", byteOffset, "byteSize", byteSize)
        return points

# Function to read and parse the binary file
def parse_binary_tree(file_path):
    #nodes = []
    proxy_node_list = []
    total_points = 0
    with open(file_path, "rb") as binary_file:
        while True:
            data = binary_file.read(22)
            if not data:
                break
            (node_type, child_mask, numPoints, byteOffset, byteSize) = struct.unpack("<BBIQQ", data)
            if node_type == 2:
                break
            total_points += numPoints
            if byteOffset + byteSize > 849367283736:
                print("Error: byteOffset + byteSize > 849367283736" + " byteOffset: " + str(byteOffset) + " byteSize: " + str(byteSize))
                exit(1)
            #print("node_type", TYPE[node_type], "child_mask", bin(child_mask), "numPoints", numPoints, "byteOffset", byteOffset, "byteSize", byteSize)


        proxy_node_list.append((node_type, child_mask, numPoints, byteOffset, byteSize))
        while True:
            data = binary_file.read(22)
            if not data:
                break
            (node_type, child_mask, numPoints, byteOffset, byteSize) = struct.unpack("<BBIQQ", data)
            if node_type == 2:
                proxy_node_list.append((node_type, child_mask, numPoints, byteOffset, byteSize))

    #n_proxy_nodes_to_printed = 0

    for proxy_node in tqdm(proxy_node_list, desc="proxy nodes"):
        total_points += print_nodes(file_path, proxy_node[3], proxy_node[4])
        #n_proxy_nodes_to_printed += 1
        #if n_proxy_nodes_to_printed > 1000:
         #   break
    print("total points", total_points)
        # iter = 0
        # while iter < 100000:
        #     data = binary_file.read(22)
        #     if not data:
        #         break
        #
        #     (node_type, child_mask, numPoints, byteOffset, byteSize) = struct.unpack("<BBIQQ", data)
        #
        #     print("node_type", TYPE[node_type], "child_mask", bin(child_mask), "numPoints", numPoints, "byteOffset", byteOffset, "byteSize", byteSize)
        #     iter += 1
            # indices = bin(child_mask)[2:].zfill(8)
            #indices = [i for i in range(8) if child_mask & (1 << i)]
            #nodes.append((indices, node_type))

    #return nodes



# Function to generate the DOT representation
def generate_dot_tree(nodes):
    dot_code = "digraph Tree {\n"
    fifo = ["r"]
    for indices, node_type in nodes:
        parent_name = fifo.pop(0)
        if indices != []:
            child_names = [parent_name + str(indices[i]) for i in range(len(indices))]
            for node_name in child_names:
                fifo.append(node_name)
                dot_code += f"  {parent_name} -> {node_name};\n"
    dot_code += "}\n"
    return dot_code

# def parse_hierarchy_bin(file_nm):
#
#     unpack_types = {
#         'int32': 'i',
#
#         'uint8': 'B',
#         'uint16': 'H',
#         'uint32': 'I',
#         'uint64': 'Q',
#
#         'double': 'd',
#     }
#
#     fields = [
#         ('type', 1, unpack_types['uint8']),
#         ('mask', 1, unpack_types['uint8']),
#         ('num_points', 4, unpack_types['uint32']),
#         ('offset', 8, unpack_types['uint64']),
#         ('size', 8, unpack_types['uint64'])]
#
#     nodes = []
#
#     with open(file_nm, 'rb') as fh:
#         #with open('/media/martijn/701ef3f0-ba07-4c68-865a-f5723880d4aa/ahn3/pointclouds/hierarchy.bin', 'rb') as fh:
#         hierarchy_raw = BytesIO(fh.read())
#         hierarchy_raw.seek(0)
#         try:
#             ct = 0
#             # while True:
#             for _ in range(100):
#                 node = []
#                 for name, read, tp in fields:
#                     value = struct.unpack('<'+ tp, hierarchy_raw.read(read))[0]
#                     if name == 'mask':
#                         print(name, value, bin(value), flush=True)
#                     else:
#                         print(name, value, flush=True)
#                     node.append(value)
#                 nodes.append(tuple(node))
#                 print("", flush=True)
#                 ct += 1
#         #            if ct > 4:
#         #                break
#         except:
#             pass
#
#     # print(nodes)
#
#     print("total point count", sum(node[2] for node in nodes))
#     print("read", ct)

if __name__ == "__main__":
    input_file  = sys.argv[1]

    #parse_hierarchy_bin(input_file)
    parsed_nodes = parse_binary_tree(input_file)
    #pprint.pprint(parsed_nodes)
    #dot_representation = generate_dot_tree(parsed_nodes)
    #tree_file = str(Path(input_file).parent) + "/tree.dot"
    #with open(tree_file, "w") as dot_file:
     #   dot_file.write(dot_representation)

    #print("DOT representation has been saved to " + tree_file)
