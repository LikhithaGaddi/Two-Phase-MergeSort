import time
import sys
import heapq
from pathlib import Path
import glob
import os

start_time = time.time()
output_file = sys.argv[2]
main_memory_size = sys.argv[3]
order = sys.argv[4]
columns = []
for i in range(5, len(sys.argv)):
    columns.append(sys.argv[i])

meta_data = open('metadata.txt', 'r')
no_of_columns = meta_data.readlines()

input_file = sys.argv[1]

columns_to_size = {}
columns_to_index = {}
size_of_each_tuple = 0
index = 0
for line in no_of_columns:
    column, size = line.split(",")
    size_of_each_tuple += int(size)
    columns_to_index[column] = index
    index += 1
    columns_to_size[column] = int(size)

size_of_each_tuple += len(list(columns_to_index.keys()))*2+1

number_of_tuples_per_list = (
    float(main_memory_size) * pow(10, 6)) // size_of_each_tuple

columns_of_list = []


def get_column(tuples):
    temp = []
    start = 0
    for col in columns_to_size.keys():
        temp.append(tuples[start:start+columns_to_size[col]])
        start += columns_to_size[col] + 2
    return temp


def heap_sort(columns_of_list):
    index = 0
    h = []
    return_value = []
    for i in range(len(columns_of_list)):
        temp = []
        for col in columns:
            temp.append(columns_of_list[i][columns_to_index[col]])
        temp.append(index)
        index += 1
        heapq.heappush(h, temp)

    for i in range(len(columns_of_list)):
        x = heapq.heappop(h)
        return_value.append(columns_of_list[x[-1]])

    if (order == "desc"):
        return_value.reverse()
    return return_value


no_of_splits = 1

index = 1
x = 0
columns_of_list = []
with open(input_file, "r") as fp:
    print("###start execution")
    print("##running Phase-1")
    size = Path(input_file).stat().st_size
    size = size // size_of_each_tuple
    no_of_splits = size // number_of_tuples_per_list
    if (size % number_of_tuples_per_list != 0):
        no_of_splits += 1
    no_of_splits = int(no_of_splits)
    print("Number of sub-files (splits):" + str(no_of_splits) + " ")
    for line in fp:
        columns_of_list.append(get_column(line))
        if x == number_of_tuples_per_list:
            print("sorting #"+str(index)+" sublist")
            filename = "file" + str(index) + ".txt"
            with open(filename, "w") as f:
                written_list = []
                columns_of_list = heap_sort(columns_of_list)
                for i in range(len(columns_of_list)):
                    columns_of_list[i] = "  ".join(columns_of_list[i])
                    written_list.append(columns_of_list[i] + "\n")
                print("Writing to disk #"+str(index))
                f.writelines(written_list)
            x = 0
            index += 1
            columns_of_list = []
        x += 1

if x != number_of_tuples_per_list:
    print("sorting #"+str(index)+" sublist")
    filename = "file" + str(index) + ".txt"
    with open(filename, "w") as f:
        written_list = []
        columns_of_list = heap_sort(columns_of_list)
        for i in range(len(columns_of_list)):
            columns_of_list[i] = "  ".join(columns_of_list[i])
            written_list.append(columns_of_list[i] + "\n")
        print("Writing to disk #"+str(index))
        f.writelines(written_list)


if (no_of_splits) >= number_of_tuples_per_list:
    print("Number of files are greater than main memory size. 2-Phase merging can't handle it")
    exit(0)

print("##running Phase-2")
print("Sorting...")

list_of_files = glob.glob('file*.txt')

flag = 1
if (order == "asc"):
    flag = 1
else:
    flag = 0


def cmp(l1, l2):
    for i in range(0, len(l1)):
        if (flag):
            if(l1[i] < l2[i]):
                return l1 < l2
        else:
            if (l1[i] > l2[i]):
                return l1 > l2


class Merge(object):
    def __init__(self, val):
        self.val = val

    def __lt__(self, other):
        return cmp(self.val, other.val)


f = open(output_file, "w")
fp = []
for i in range(0, (no_of_splits)):
    filename = "file" + str(i+1) + ".txt"
    # print(filename)
    x = open(filename, "r")
    fp.append(x)


h = []


def push(s, i):
    if (len(s) == 0):
        return
    s = get_column(s)
    temp = []
    for col in columns:
        temp.append(s[columns_to_index[col]])
    temp.append(i)
    file_to_record[i] = s
    heapq.heappush(h, Merge(temp))


def pop():
    p = heapq.heappop(h)
    p = p.val
    # print(p)
    file_no = p[-1]
    p = "  ".join(file_to_record[file_no])+"\r\n"
    return [p, file_no]


file_to_record = {}
for i in range(len(fp)):
    s = fp[i].readline()
    push(s, i)

print("Writing to disk")
while len(h) != 0:
    p, file_no = pop()
    f.write(p)
    s = fp[file_no].readline()
    if len(s) > 0:
        push(s, file_no)

for i in range(1, (len(list_of_files))):
    filename = "file" + str(i) + ".txt"
    # print(filename)
    os.remove(filename)


print("###completed execution")
f.close()
end_time = time.time()
# print(end_time - start_time)
