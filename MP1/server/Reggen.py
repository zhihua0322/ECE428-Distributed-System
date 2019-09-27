from xeger import Xeger

print("Generating file")
x = Xeger(limit=50)

f = open("test.log", 'w')
for i in range(111):
    f.write(x.xeger("(^[0-9]+[a-z])\n"))
for i in range(222):
    f.write("Mozilla\n")
for i in range(333):
    f.write("www\n")
for i in range(444):
    f.write(x.xeger("((a)(b)(ac).* (a)(b)(ac))\n"))

print("Finished")