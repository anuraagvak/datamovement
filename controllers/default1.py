# coding: utf8
# try something like poooooooooooda
import os
import subprocess
import MySQLdb
import json
Table = "login";
machine="localhost";
database_name="hadoop";
db_username="root";
password="hadoop";
def index():
#    return dict(a=1);
    redirect(URL('db_combination'));
#    redirect(URL('all_jobs'));

    return dict(message="hello from default.py")
def compile1():
    return dict(hello="hello");
tkns=[]
codegen1="";
def codegen():
    print "the tables is"
    print Table;
#    //codegen:          generate import-export code without performing actual import/export, modify the gnerated java file, compile it to jar, give it to sqoop.
#    global $TABLE,$machine,$password,$db_username,$database_name;
    global codegen1;
    try:
    	os.remove(Table+".java");
    except:
    	print ""
    try:
    	os.remove(Table+".jar");
    except:
    	print ""
    try:
    	os.remove(Table+".class")
    except:
    	print ""
    codegen1= "sqoop codegen --connect jdbc:mysql://"+machine+"/"+database_name+" --username "+db_username+" --password '"+password+"' --table "+Table ;
    """
        if posvars['op']=="import":
            if posvars['field_delim']!="":
                codegen1=codegen1+" --fields-terminated-by "+"'" +posvars['field_delim']+"'";
            if posvars['line_delim']!="":
                codegen1=codegen1+" --lines-terminated-by "+"'" +posvars['line_delim']+"'";
            if 'enclosed' in posvars:
                if posvars['enclosingchar']=='"':
                    posvars['enclosingchar']='\"';
                    codegen1=codegen1+" "+posvars['enclosed']+" '"+posvars['enclosingchar']+ "'";
    """
    codegen1 = codegen1+"  2> err ; echo $?";

    output = os.popen(codegen1).read()

def codechange(table,list_encrypt,list_mask,jsonobj):
	f = open(table+'.java');
	lines = f.readlines();
	f.close();

	f = open('encrypt_decrypt_functions.txt');
	lines1 = f.readlines();
	f.close();

	f = open('mask.txt');
	lines2 = f.readlines();
	f.close();

	lines_mask = [];
	no_col_mask = len(list_mask);
	print 'i is'

	for i in list_mask:
		print i
		print jsonobj['hadoop'][table][i]['mask']
		list_maps = jsonobj['hadoop'][table][i]['mask'].split(',');
		temp11 = "";
		for j in list_maps:
			temp11 = temp11 + 'mask_strings.put("' + j.split(':')[0] + '", "' + j.split(':')[1] + '");'
		print temp11
			
		temp_list = ['public String mask_'+i+'(final String text){\n'] + [lines2[0]] + [temp11] +lines2[1:];
		lines_mask = lines_mask+temp_list;

	connection = MySQLdb.connect(host='localhost',user='root', passwd='hadoop', db=database_name)
	cur=connection.cursor()
	query = "desc "+table;
	cur.execute(query)
	tables = cur.fetchall()
	a = [];
	for i in tables:
		a.append(i[0]);

	list_str=[];
	list_int=[];

	write_file=""
	flag1 = 1;
	str1 = "";
	str2 = ''.join(lines_mask) + ''.join(lines1);

	for l in lines:
		str1 = l;
		flag = 0;
		if "SqoopRecord  implements DBWritable, Writable" in l:
			str1=str1+str2;

		if "get_" in l:
			if "String" in l:
				ind1 = l.index("get_");
				ind2 = l[ind1+4:].index("(");
				list_str.append(l[ind1+4:ind1+4+ind2]);
			
			if "Integer" in l:
				ind1 = l.index("get_");
				ind2 = l[ind1+4:].index("(");
				list_int.append(l[ind1+4:ind1+4+ind2]);

		if flag1 == 1:
			for i in list_str:
				if "this."+i in l and i in list_encrypt:
					ind1 = l.index("JdbcWritableBridge");
					ind2 = l.index(";");
					str1 = l[:ind1]+"encrypt1("+l[ind1:ind2]+");";	#encrypt1 is for string
					flag = 1;
					break;
				if flag == 1:
					break;
			for i in list_int:
				if "this."+i in l and i in list_encrypt:
					ind1 = l.index("JdbcWritableBridge");
					ind2 = l.index(";");
					str1 = l[:ind1]+"encrypt2("+l[ind1:ind2]+");";	#encrypt1 is for string
					flag = 1;
					break;
				if flag == 1:
					break;

		if flag1 == 1 and "}" in l:
			flag1 = 0;

		if "cur_result_set = __dbResults;" in l:
			flag1 = 1;


		if "JdbcWritableBridge.writeInteger" in l:
			ind1 = l.index("JdbcWritableBridge.writeInteger");
			a = l[ind1:];
			d = a.split(',');
			if d[0][32:] in list_encrypt:
				str1 = a[:32]+'decrypt2('+d[0][32:]+')'+a[len(d[0]):];
			else:
				str1 = l;

		if "JdbcWritableBridge.writeString" in l:
			ind1 = l.index("JdbcWritableBridge.writeString");
			a = l[ind1:];
			d = a.split(',');
			if d[0][31:] in list_encrypt:
				str1 = a[:31]+'decrypt1('+d[0][31:]+')'+a[len(d[0]):];
			else:
				str1 = l;

		write_file=write_file+str1;
		f = open(table+'.java',"w");
		f.write(write_file);
		f.close();

def db_combination():
	return dict(a=1);
	


def all_jobs1():
	return dict(a=1);

def all_jobs():
    jobs=[];
    output = os.popen(""" jps | grep Jps | awk '{print $1;}' """).read()
    output = output.split('\n')[:-1];
    try :
        connection = MySQLdb.connect(host='localhost',user='root', passwd='hadoop', db='jobs')
        cur=connection.cursor();
        query = "select * from db where result = 0 AND job_status = 1";
        cur.execute(query)
        tables = cur.fetchall()
        cur1 = connection.cursor();
        query = "select * from db where job_status = 0";
        cur1.execute(query)
        tables2 = cur1.fetchall()
        cur2 = connection.cursor();
        query = "select * from db where result != 0";
        cur2.execute(query)
        tables3 = cur2.fetchall()
#        max_id = tables[0][0];
    except:
        print "weee2"
    return dict(L = tables,running = tables2,failed=tables3,db_type="sql");
def sql1():
    tables = [];
    path = '/'
    if 'path' in request.get_vars:
        path = request.get_vars['path'];
    a = "hadoop fs -ls "+path+" | tr -s ' ' | cut -d ' ' -f8"
    output = os.popen(a).read()
    tokens = output.split('\n')
    error = ""
    try :
        connection = MySQLdb.connect(host='localhost',user='root', passwd='hadoop', db=database_name)
        cur=connection.cursor()
        query = "show tables"
        cur.execute(query)
        tables = cur.fetchall()
        dic ={}
        for (t,) in tables:
                    
                    cur = connection.cursor()
                    
                    query = "desc "+t +";";
                    cur.execute(query);
                    
                    columns = cur.fetchall()
                    dic[t] = []
                    for i in columns:
                            dic[t].append(i[0])
#                    print dic[t]
        return dict(tokens=tokens,error=error,tables=tables,test = request.get_vars,dic=dic)
    except:
        print "weee"
        error = "failed to connect to MySQL: "
        return dict(tokens=tokens,error=error,tables=tables,dic=dic)
def hdfs1():
    tables = [];
    path = '/'
    if 'path' in request.get_vars:
        path = request.get_vars['path'];
    a = "hadoop fs -ls "+path+" | tr -s ' ' | cut -d ' ' -f8"
    output = os.popen(a).read()
    tokens = output.split('\n')
    error = ""
    try :
        connection = MySQLdb.connect(host='localhost',user='root', passwd='hadoop', db=database_name)
        cur=connection.cursor()
        query = "show tables"
        cur.execute(query)
        tables = cur.fetchall()
        return dict(tokens=tokens,error=error,tables=tables,test = request.get_vars)
    except:
        print "weee"
        error = "failed to connect to MySQL: "
        return dict(tokens=tokens,error=error,tables=tables)
def main():
    if request.get_vars['dbtype'] == "sql":
        redirect(URL('sql1'));
    elif request.get_vars['dbtype'] == "hdfs":
        redirect(URL('hdfs1'));

def main1():
    if request.get_vars['dbtype'] == "sql":
        redirect(URL('sql2'));
    elif request.get_vars['dbtype'] == "cassandra":
        redirect(URL('cassandra2'));


def sql2():
	return dict(a=100);


def hdfs():
    #this is export
    present_pid = os.getpid();
    global codegen1;
    posvars = request.post_vars;
    posvars['op'] = "export";
    comm = "sqoop "+posvars['op'];
    print posvars['updateid'];

    max_id = 0;
    try :
        connection = MySQLdb.connect(host='localhost',user='root', passwd='hadoop', db='jobs')
        cur=connection.cursor();
        query = "insert into db(operation,table_transfered,start_time,job_status) values ('" + posvars['op'] + "'," + "'" + posvars['table'] +"'," + "NOW(),0);";
        print query;
        cur.execute(query)
        connection.commit();
        cur1=connection.cursor();
        query = """ select max(id) from db;  """;
        cur1.execute(query)
        tables = cur1.fetchall()
        max_id = tables[0][0];
    except:
        print "weee2"
        
    imp = "";




    if posvars['op'] == "export":
#        comm=comm + " --connect jdbc:mysql://"+machine+"/"+database_name+" --username "+db_username+" --password '"+password+"' --table "+posvars['table']+" "
    	json_data=open('sar.json','r')
	data = json.load(json_data)
	print data['hadoop']
	json_data.close()
    	tables_encrypt = [];
    	for i in data['hadoop'][posvars['table']]:
		if data['hadoop'][posvars['table']][i] == "encrypt":
			tables_encrypt.append(i);
			posvars['decrypt'] = 1;
			

	if 'decrypt' in posvars:
	        global Table;
	    	Table = posvars['table'];
                codegen();
		codechange(Table,tables_encrypt);
		compile_code = """ /usr/lib/jvm/java-7-oracle/bin/javac -classpath /home/hduser/sqoop/sqoop-1.4.4.jar:/home/hduser/hadoop/hadoop-core-1.2.1.jar: """ + Table+".java; /usr/lib/jvm/java-7-oracle/bin/jar -cf "+Table+".jar "+Table+".class ";
		output = os.popen(compile_code).read();
                imp="  --jar-file "+Table+".jar --class-name "+Table;
         
	comm=comm + " --connect jdbc:mysql://"+machine+"/"+database_name+" --username "+db_username+" --password '"+password+"' --table "+posvars['table']+" "+imp;


        if 'directory' in posvars:
            comm=comm+" --export-dir "+posvars['directory'];

        if posvars['updateid']!="" and posvars['updateid']!="Enter reference column to update the table":
            comm=comm+" --update-key "+posvars['updateid'] + " --update-mode allowinsert";

        print "Export command="+comm+" <br>";

        comm = comm+ """ ;mysql --user=root --password=hadoop -e "use jobs;update db set result=$?,job_status=1  where id= """+ str(max_id) +""" AND job_status=0 "; exit 1 """;
        file1 = open(str(present_pid),'w');
        print comm;
        file1.write(comm);
        file1.close();
        pid = os.fork();
        if pid != 0:
                try :
                        connection = MySQLdb.connect(host='localhost',user='root', passwd='hadoop', db='jobs')
                        cur=connection.cursor();
                        query = "update db set pid="+ str(pid+1) + " where id="+str(max_id)+";";
                        cur.execute(query)
                        connection.commit();
                        tables = cur.fetchall()
                except:
                        print "weee1"

        else:
                os.execlp('bash','bash',str(present_pid));

    val='0';#output[output.__len__()-2];
    redirect(URL('all_jobs'))


def sql():
    print "came to sql"
    json_data=open('sar.json','r')
    data = json.load(json_data)
    print data['hadoop']
    json_data.close()
    #this is import
    present_pid = os.getpid();
    global codegen1;
    posvars = {};
    posvars['op'] = "import";
    print posvars;
    first_flag = 0;
    all_tables = ['login']
    print "before"
    for tab in all_tables:
        first_flag = first_flag+2;
        posvars['table'] = tab;
        max_id = 0;
        print posvars['table'];
	"""
        try :
            print "try came"
            print posvars['table'];
            connection = MySQLdb.connect(host='localhost',user='root', passwd='hadoop', db='jobs')
            cur=connection.cursor();
            query = "insert into db(operation,table_transfered,start_time,job_status) values ('" + posvars['op'] + "'," + "'" + posvars['table'] +"'," + "NOW(),0);";
            cur.execute(query)
            connection.commit();
            cur1=connection.cursor();
            cur1.execute(query)
            tables = cur1.fetchall()
            max_id = tables[0][0];
        except:
            print "weee2"

	"""

        if posvars['op'] == "import":
            imp="";
    	    tables_encrypt = [];
	    tables_mask = [];
       	    for i in data['hadoop'][posvars['table']]:
		if data['hadoop'][posvars['table']][i] == "encrypt":
			tables_encrypt.append(i);
			posvars['encrypt'] = 1;	
		if "mask" in data['hadoop'][posvars['table']][i]:
			tables_mask.append(i);
			posvars['mask'] = 1;
	

	    print tables_encrypt;

    	    if 'encrypt' in posvars or 'mask' in posvars:
	        global Table;
	    	Table = posvars['table'];
                codegen();
#                array=posvars['encrypt_columns'].split(',');#explode(",",$_POST['encrypt_columns']);
                #print_r($array);
                #change_import($TABLE.".java",$array,$_POST['enckey']);
#                compile1();
		codechange(Table,tables_encrypt,tables_mask,data);
		compile_code = """ /usr/lib/jvm/java-7-oracle/bin/javac -classpath /home/hduser/sqoop/sqoop-1.4.4.jar:/home/hduser/hadoop/hadoop-core-1.2.1.jar: """ + Table+".java; /usr/lib/jvm/java-7-oracle/bin/jar -cf "+Table+".jar "+Table+".class ";
#		output = os.popen(compile_code).read();

                imp="  --jar-file "+Table+".jar --class-name "+Table;

	    comm = "";
            comm=comm + " --connect jdbc:mysql://"+machine+"/"+database_name+" --username "+db_username+" --password '"+password+"' --append --table "+posvars['table']+" "+imp;
            print query;
            print "anath"
#            print posvars['tab_columns'];
            if type(posvars['tab_columns']) == type("apple"):
                posvars['tab_columns'] = [posvars['tab_columns']];
            columns = [];
            yes = 0;
            if 'tab_columns' in posvars:
                for i in posvars['tab_columns']:
                    a = i.split('$');
                    if a[0] == posvars['table']:
                        yes = 1;
                        columns.append(a[1]);
                if yes == 1:
                    comm=comm+""" --columns " """ + columns[0];
                for i in range(1,len(columns)):
                    comm=comm+" ,"+columns[i];
                comm=comm+""" " """;

            if 'filetype' in posvars:
                comm=comm+ " "+posvars['filetype'];

            comm = comm+ """ ; mysql --user=root --password=hadoop -e "use jobs;update db set result=$?,job_status=1  where id= """+ str(max_id) +""" AND job_status=0 "; exit 1 """;
            print comm;
	    """
            file1 = open(str(present_pid),'w');
            file1.write(comm);
            file1.close();
            print present_pid;
            pid = os.fork();
            if pid != 0:
                    print pid;
                    try :
                            connection = MySQLdb.connect(host='localhost',user='root', passwd='hadoop', db='jobs')
                            cur=connection.cursor();
                            query = "update db set pid="+ str(pid+1) + " where id="+str(max_id)+";";
                            cur.execute(query)
                            connection.commit();
                            tables = cur.fetchall()
                    except:
                            print "weee1"
    
            else:
                    os.execlp('bash','bash',str(present_pid));
	    """
#    val='0';#output[output.__len__()-2];
#    redirect(URL('all_jobs'))
#    return dict(posvars = request.post_vars);
def user():
    return dict(form=auth())

sql();
