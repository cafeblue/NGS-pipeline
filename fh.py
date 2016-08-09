#! /bin/env python3

f = open("test.txt", 'w')
f.write("col1\tcol2\tcol3\n")
f.write("cont1\tcont2\tcont3\n")
f.close()

num_lines = sum(1 for line in open('test.txt'))
print(str(num_lines) + "\n")

for line in open("test.txt", 'r'):
    line = line.rstrip()
    print("a")
    print("Line:\t" + str(line))
    print("b")
