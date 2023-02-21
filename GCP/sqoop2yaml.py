import yaml
import sys

with open('mysqoop2.txt', 'r') as file:
     f1 = file.read()
f2 = f1.splitlines()  #split the file by eol character into a list.
yamllist = []
for f in f2:
    mylist = []
    mydict = {}
    s1 = f.split('--')
   
    tbl = s1[5].replace(' ',':',1).split('from')[1].split('where')[0]  # split the query to get just the table name
    server = s1[1].replace(' ',':',1).split('//')[1].split('.')[0]  # split the query to get just the table name
    gcptable = s1[12].replace(' ',':',1).split(':')[1] 
    mylist.append(f'tablename:{tbl.strip()}')  # tablename
    mylist.append(f'gcptable:{gcptable.strip()}')  # gcptable
    mylist.append('datecolumn:  ')  # date column
    mylist.append(f'query: select * from {tbl}')  # query
    mylist.append('action:truncate')  # gcptable
    mylist.append(f'server:{server.strip()}')  # server
    mylist.append(f'dntl: ')  # server
   # mylist.append(s1[5].replace(' ',':',1).replace("'",'').strip())  # connect
    
    for d in mylist:    # create a dictionary item for each line.
         
        (key,value) = d.split(':',1)
        mydict[key] = value
   
    yamllist.append(mydict) 
   
with open(r'output2.yaml', 'w') as file:

    outputs = yaml.dump(yamllist, file)
   
exit()