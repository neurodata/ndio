file_location = '/etc/nginx/sites-enabled/default'
# file_location = './default'

should_continue = True
infile = open(file_location,'r')
for line in infile:
    lineArray = line.split()
    if len(lineArray) == 3:
        if lineArray[1] == '/testserver/':
            should_continue = False
infile.close()

if (should_continue):
    infile = open(file_location, 'r')
    ls = []
    lines = infile.readlines()
    position = 0
    for line in lines:
        lineArray = line.split()
        if len(lineArray) == 2:
            if lineArray[0] == 'server_name' and lineArray[1] == 'localhost;':
                ls.append(position+1)
        position += 1

    infile.close()

    a = "   location /testserver/ {\n"
    b = "       alias /tmp/;\n"
    c = "       autoindex on;\n"
    d = "       allow all;\n      }\n\n"

    put_me = a + b + c + d
    lines.insert(ls[0],put_me)

    infile = open(file_location, 'w')
    infile.write("".join(lines))
    infile.close()
