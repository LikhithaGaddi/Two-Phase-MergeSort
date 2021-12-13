import time
import sys
import heapq
from pathlib import Path
import glob
import os
import threading


start_time = time.time()
output_file = sys.argv[2]
main_memory_size = sys.argv[3]
order = sys.argv[5]
number_of_threads = int(sys.argv[4])
columns = []
for i in range(6, len(sys.argv)):
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

number_of_tuples_per_list = (
    float(main_memory_size) * pow(10, 6)) // size_of_each_tuple
number_of_lines_per_thread = int(
    number_of_tuples_per_list // number_of_threads)

columns_of_list = []


def get_line_size():
    f = open(input_file, "r")
    size = len(f.readline()) + 1
    f.close()
    return size


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


mutex = threading.Lock()


def read_and_sort(index, file_pointer, start, line_size, number_of_lines):
    filename = "file" + str(index) + ".txt"
    mutex.acquire()
    columns = []
    for i in range(number_of_lines):
        # print(start)
        start += line_size
        # file_pointer.seek(start)
        line = file_pointer.readline()
        if len(line) > 0:
            columns.append(get_column(line))
            # print(line[:-1])
        else:
            break

    mutex.release()

    with open(filename, "w") as f:
        written_list = []
        print("sorting #"+str(index)+" sublist")
        columns = heap_sort(columns)
        for i in range(len(columns)):
            columns[i] = "  ".join(columns[i])
            written_list.append(columns[i] + "\n")
        print("Writing to disk #"+str(index))
        f.writelines(written_list)


class myThread (threading.Thread):
    def __init__(self, index, file_pointer, start1, line_size, number_of_lines):
        threading.Thread.__init__(self)
        self.index = index
        self.file_pointer = file_pointer
        self.start1 = start1
        self.line_size = line_size
        self.number_of_lines = number_of_lines

    def run(self):
        read_and_sort(self.index,
                      self.file_pointer,
                      self.start1,
                      self.line_size,
                      self.number_of_lines)


no_of_splits = 1

index = 1
columns_of_list = []
fp = open(input_file, "r")
print("###start execution")
print("##running Phase-1")


size = Path(input_file).stat().st_size
print(size)
size = size // size_of_each_tuple
no_of_splits = size // number_of_tuples_per_list
if (size % number_of_lines_per_thread != 0):
    no_of_splits += 1

line_size = get_line_size()
no_of_splits = int(no_of_splits)

print("Number of sub-files (splits):" +
      str(no_of_splits * (number_of_threads-1)) + " ")
# print(number_of_lines_per_thread)

for split_no in range(no_of_splits):

    threads = []
    start1 = int(split_no * number_of_tuples_per_list * line_size)

    for thread_no in range(number_of_threads-1):
        start1 = int(start1 + thread_no*number_of_lines_per_thread*line_size)
        thread = myThread(index, fp, start1,
                          line_size, number_of_lines_per_thread)

        threads.append(thread)
        thread.start()
        index += 1

    for thread in threads:
        thread.join()


fp.close()


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
for i in range(len(list_of_files)):
    filename = "file" + str(i+1) + ".txt"
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


for i in range(len(list_of_files)):
    filename = "file" + str(i+1) + ".txt"
    os.remove(filename)


print("###completed execution")
f.close()
end_time = time.time()
print(end_time - start_time)
