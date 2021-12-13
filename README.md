command to generate input: ./gensort -a <file_size> <file_name>
command to execute sorting without threads : python3 sort.py <input_file> <output_file> <main_memory_size_in_MB> <order_asc_or_desc> <columns>
	example : python3 sort.py "input.txt" "output.txt" 5 asc C1
command to execute sorting with threads : python3 sort1.py <input_file> <output_file> <main_memory_size_in_MB> <number_of_threads> <order_asc_or_desc> <columns>
	example : python3 sort1.py "input.txt" "output.txt" 5 3 asc C1

metadata.txt

<column_name>,<size_in_bytes>

