# coding: utf8
# try something like poooooooooooda
import os
import subprocess
import MySQLdb
import json
import urllib2
is_hdfs_to_cassandra = 0
hdfs_cass_posvars = {}
Table = "workflow";
machine="localhost";
database_name="hadoop";
db_username="root";
password="hadoop";
policy_name = "anuraag1"
@auth.requires_login()
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
    posvars=request.post_vars;
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
	print jsonobj;
	f = open(table+'.java');
	lines = f.readlines();
	f.close();

	f = open('encrypt_decrypt_functions.txt');
	lines1 = f.readlines();
	f.close();

	f = open('mask.txt');
	lines2 = f.readlines();
	f.close();

	f = open('unmask.txt');
	lines3 = f.readlines();
	f.close();

	lines_mask = [];
	lines_mask1 = [];



	for i in list_mask:
		list_maps = jsonobj['hadoop'][table][i]['mask'].split(',');
		temp11 = "";
		temp22 = "";
		for j in list_maps:
			temp11 = temp11 + 'mask_strings.put("' + j.split(':')[0] + '", "' + j.split(':')[1] + '");'
			temp22 = temp22 + 'mask_strings.put("' + j.split(':')[1] + '", "' + j.split(':')[0] + '");'
		print temp11
		print temp22
			
		temp_list = ['public String mask_'+i+'(final String text){\n'] + [lines2[0]] + [temp11] +lines2[1:];
		temp_list1 = ['public String unmask_'+i+'(final String text){\n'] + [lines3[0]] + [temp22] +lines3[1:];
		lines_mask = lines_mask+temp_list;
		lines_mask1 = lines_mask1+temp_list1;

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
	str2 = ''.join(lines_mask) + ''.join(lines_mask1) + ''.join(lines1);
	for l in lines:
		str1 = l;
		flag = 0;
		if "SqoopRecord  implements DBWritable, Writable" in l:
			str1="import java.util.HashMap; \n"+str1+str2;

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

				if "this."+i in l and i in list_mask:
					ind1 = l.index("JdbcWritableBridge");
					ind2 = l.index(";");
					str1 = l[:ind1]+"mask_"+i+"("+l[ind1:ind2]+");";	#encrypt1 is for string
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
			elif d[0][31:] in list_mask:
				str1 = a[:31]+'unmask_'+d[0][31:]+'('+d[0][31:]+')'+a[len(d[0]):];
			else:
				str1 = l;

		write_file=write_file+str1;
		f = open(table+'.java',"w");
		f.write(write_file);
		f.close();


@auth.requires_login()
def db_combination():
	return dict(a=1);
	


@auth.requires_login()
def all_jobs1():
#	os.popen('python ~/web2py/ex.py');
    return dict(L = [],running = [],failed=[],db_type="sql");
#	return dict(a=1);

@auth.requires_login()
def all_jobs2():
    return dict(L = [],running = [],failed=[],db_type="sql");


@auth.requires_login()
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
@auth.requires_login()
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
        print tables
        dic ={}
        for (t,) in tables:
                    
                    cur = connection.cursor()
                    
                    query = "desc "+t +";";
                    cur.execute(query);
                    
                    columns = cur.fetchall()
                    dic[t] = []
                    for i in columns:
                            dic[t].append(i[0])
        print dic
#                    print dic[t]
        return dict(tokens=tokens,error=error,tables=tables,test = request.get_vars,dic=dic)
    except:
        print "weee"
        error = "failed to connect to MySQL: "
        return dict(tokens=tokens,error=error,tables=tables,dic=dic)
@auth.requires_login()
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
@auth.requires_login()
def main():
    if request.get_vars['dbtype'] == "sql":
        redirect(URL('sql1'));
    elif request.get_vars['dbtype'] == "hdfs":
        redirect(URL('hdfs1'));

@auth.requires_login()
def main1():
    if request.get_vars['dbtype'] == "sql":
        redirect(URL('sql2'));
    elif request.get_vars['dbtype'] == "cassandra":
        redirect(URL('cassandra2'));


def sql2():
	return dict(a=100);

#hdfs to mysql

@auth.requires_login()
def hdfs():
    global is_hdfs_to_cassandra;
    global hdfs_cass_posvars
    posvars = request.post_vars;
    print posvars;
    if 'policy_name' not in posvars:
        posvars['policy_name'] = "anuraag1"
    json_data=open('/home/hduser/web2py/applications/Policy_server/json_files/'+posvars['policy_name']+'.json','r')
    data = json.load(json_data)
    print data['hadoop']
    json_data.close()
    #this is export
    present_pid = os.getpid();
    global codegen1;
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
    	tables_encrypt = [];
    	tables_mask = [];
	if posvars['directory'][1:] not in data['hadoop']:
		data['hadoop'][posvars['directory'][1:]] = {};
	print posvars['directory'][1:]
	print data

    	for i in data['hadoop'][posvars['directory'][1:]]:
		print i
		if data['hadoop'][posvars['directory'][1:]][i] == "encrypt":
			tables_encrypt.append(i);
			posvars['decrypt'] = 1;

		if "mask" in data['hadoop'][posvars['directory'][1:]][i]:
			tables_mask.append(i);
			posvars['mask'] = 1;
			
        if 'decrypt' in posvars or 'mask' in posvars:
	        global Table;
	    	Table = posvars['directory'][1:];
                codegen();
		codechange(Table,tables_encrypt,tables_mask,data);
		compile_code = """ /usr/lib/jvm/java-7-oracle/bin/javac -classpath /home/hduser/sqoop/sqoop-1.4.4.jar:/home/hduser/hadoop/hadoop-core-1.2.1.jar: """ + Table+".java; /usr/lib/jvm/java-7-oracle/bin/jar -cf "+Table+".jar "+Table+".class ";
		output = os.popen(compile_code).read();
                imp="  --jar-file "+Table+".jar --class-name "+Table;
         
	comm=comm + " --connect jdbc:mysql://"+machine+"/"+database_name+" --username "+db_username+" --password '"+password+"' --table "+posvars['table']+" "+imp;


        if 'directory' in posvars:
            comm=comm+" --export-dir "+posvars['directory'];

        if posvars['updateid']!="" and posvars['updateid']!="Enter reference column to update the table":
            comm=comm+" --update-key "+posvars['updateid'] + " --update-mode allowinsert";

        print "Export command="+comm+" <br>";

        comm = comm+ """ ;mysql --user=root --password=hadoop -e "use jobs;update db set result=$?,job_status=1  where id= """+ str(max_id) +""" AND job_status=0 "; """;
        
        quotes = """ " """
        quotes = quotes.split()[0]
        if is_hdfs_to_cassandra == 1:
            print "hdfs_cass_posvars"
            print hdfs_cass_posvars;
            comm = comm + "echo hadoop | sudo -S rm /tmp/data.txt; mysql --user=root --password=hadoop -e "+quotes+quotes+quotes+ """ use hadoop;SELECT * from """ + hdfs_cass_posvars['table'] +""" INTO OUTFILE '/tmp/data.txt' fields terminated by ','; """ +quotes+quotes+quotes+""" ; echo hadoop | sudo -S mv /tmp/data.txt /home/hduser/. """;
            comm = comm + """ ; cqlsh -e " use hadoop; copy """ + hdfs_cass_posvars['table1'] + """ from '/home/hduser/data.txt' " """

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
    if is_hdfs_to_cassandra == 1:
        print "in the hdfs() thing"
        is_hdfs_to_cassandra = 0
        return;
    redirect(URL('all_jobs'))

#mysql to hdfs

@auth.requires_login()
def sql():
    global is_cassandra_to_hdfs;
    global cass_hdfs_posvars;
    posvars = request.post_vars;
    if 'policy_name' not in posvars:
        posvars['policy_name'] = "anuraag1"
    print "came to sql"
    print posvars
    json_data=open('/home/hduser/web2py/applications/Policy_server/json_files/'+posvars['policy_name']+'.json','r')
    data = json.load(json_data)
    print data['hadoop']
    json_data.close()
    #this is import
    present_pid = os.getpid();
    global codegen1;
    posvars['op'] = "import";
    all_tables = posvars['table'];
    print posvars;
    print type(all_tables);
    if type(all_tables)==type("str"):
        all_tables=[all_tables];
    first_flag = 0;
    for tab in all_tables:
        comm = "sleep "+ str(first_flag) +" ;sqoop "+posvars['op']+" ";
        first_flag = first_flag+2;
        posvars['table'] = tab;
        max_id = 0;
        print "before"
        print posvars['table'];
        try :
            print "try came"
            print posvars['table'];
            connection = MySQLdb.connect(host='localhost',user='root', passwd='hadoop', db='jobs')
            cur=connection.cursor();
            query = "insert into db(operation,table_transfered,start_time,job_status) values ('" + posvars['op'] + "'," + "'" + posvars['table'] +"'," + "NOW(),0);";
            cur.execute(query)
            connection.commit();
            cur1=connection.cursor();
            query = """ select max(id) from db;  """;
            cur1.execute(query)
            tables = cur1.fetchall()
            max_id = tables[0][0];
        except:
            print "weee2"

        if posvars['op'] == "import":
            imp="";
    	    tables_encrypt = [];
	    tables_mask = [];
	    if posvars['table'] not in data['hadoop']:
		    data['hadoop'][posvars['table']] = {};
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
		output = os.popen(compile_code).read();

                imp="  --jar-file "+Table+".jar --class-name "+Table;

            comm=comm + " --connect jdbc:mysql://"+machine+"/"+database_name+" --username "+db_username+" --password '"+password+"' --append --table "+posvars['table']+" "+imp;
            if posvars['targetdir1'] != "":
                comm=comm+" --target-dir /"+posvars['targetdir1'];
            elif 'targetdir' in posvars:
                if posvars['targetdir']=='default':
                    comm=comm+" --target-dir /"+posvars['table'];
                elif posvars['targetdir']!="":
                    comm=comm+" --target-dir "+posvars['targetdir'];
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
            file1 = open(str(present_pid),'w');
            file1.write(comm);
            file1.close();
            print comm;
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

    val='0';#output[output.__len__()-2];
    if is_cassandra_to_hdfs == 1:
        is_cassandra_to_hdfs = 0
        return;
    redirect(URL('all_jobs'))
#    return dict(posvars = request.post_vars);
def user():
    return dict(form=auth())


@auth.requires_login()
def cassandratohdfs():
    tables = [];
    path = '/'
    if 'path' in request.get_vars:
        path = request.get_vars['path'];
    a = "hadoop fs -ls "+path+" | tr -s ' ' | cut -d ' ' -f8"
    output = os.popen(a).read()
    tokens = output.split()
    error = ""
    try :
        b = """ cqlsh -e " use hadoop;describe tables " """
        print b;
        output = os.popen(b).read()
        tables = output.split();
        print tables
        dic ={}
        for t in tables:
            query = """ cqlsh -e " use hadoop;describe columnfamily """ + t + """ " """;
            output = os.popen(query).read()
            output = output.split(',')
            output[0] = output[0].split('(')[1]
            print output
            dic[t] = []

            for i in output:
                if len(i.split()) == 2:
                    print i.split()[0]
                    dic[t].append(i.split()[0])
            
            print dic
        return dict(tokens=tokens,error=error,tables=tables,test = request.get_vars,dic=dic)
    except:
        print "weee"
        error = "failed to connect to MySQL: "
        return dict(tokens=tokens,error=error,tables=tables,dic=dic)

    return dict(message="hello from hdfstocassandra.py")


@auth.requires_login()
def cassandratohdfs2():
    global is_cassandra_to_hdfs;
    global cass_hdfs_posvars;
    is_cassandra_to_hdfs = 1
    posvars=request.post_vars;
    cass_hdfs_posvars = {'table':posvars['table'], 'table1':posvars['table']};

    comm = """ cqlsh -e " copy hadoop."""+posvars['table']+""" to 'temp.csv' " """
    print comm
    result = os.popen(comm).read()

    #csv file to mysql
    quotes = """ " """
    quotes = quotes.split()[0]
    comm = "echo hadoop | sudo -S  mv temp.csv /var/lib/mysql/hadoop/.;  mysql --user=root --password=hadoop -e " +quotes+quotes+quotes+ """ LOAD DATA INFILE 'temp.csv' INTO TABLE hadoop.""" + posvars['table'] +""" FIELDS TERMINATED BY ','; """ +quotes+quotes+quotes;
    print comm
    result = os.popen(comm).read();

    sql()
    print "returned "


@auth.requires_login()
def hdfstocassandra2():
    global is_hdfs_to_cassandra;
    global hdfs_cass_posvars;
    is_hdfs_to_cassandra = 1
    posvars=request.post_vars;
    hdfs_cass_posvars = {'table':posvars['table'], 'table1':posvars['table']};
    hdfs()
    print "returned "
#    mysqltocassandra2();

@auth.requires_login()
def hdfstocassandra222222222():
    print "hdfstocassandra2"
    posvars=request.post_vars
    quotes = """ " """
    quotes = quotes.split()[0]
    print quotes;
    
    query = """ cqlsh -e " use hadoop;describe columnfamily """ + t + """ " """;
    output = os.popen(query).read()
    output = output.split(',')
    output[0] = output[0].split('(')[1]
    print output
    dic[t] = []
    #    comm = """ cqlsh -e """ + quotes+quotes+quotes +"""INSERT INTO hadoop.""" + posvars['table'] + """  (id,email,org_id,password,status,username)   VALUES ('999', 'guru@gmail.com', 'rajapalayam','asdfasdf','not','guru')  """+quotes+quotes+quotes;

    """
    for i in output:
        if len(i.split()) == 2:
            print i.split()[0]

    print comm;
    file1 = open('run.sh','w');
    file1.write(comm);
    file1.close();
    """
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
        print 'came to else'
        os.execlp('bash','bash','script.sh');


    print posvars
    redirect(URL('db_combination'))

@auth.requires_login()
def hdfstocassandra():
    tables = [];
    path = '/'
    if 'path' in request.get_vars:
        path = request.get_vars['path'];
    a = "hadoop fs -ls "+path+" | tr -s ' ' | cut -d ' ' -f8"
    output = os.popen(a).read()
    tokens = output.split()
    error = ""
    try :
        b = """ cqlsh -e " use hadoop;describe tables " """
        print b;
        output = os.popen(b).read()
        tables = output.split();
        print tables
        dic ={}
        for t in tables:
            query = """ cqlsh -e " use hadoop;describe columnfamily """ + t + """ " """;
            output = os.popen(query).read()
            output = output.split(',')
            output[0] = output[0].split('(')[1]
            print output
            dic[t] = []

            for i in output:
                if len(i.split()) == 2:
                    print i.split()[0]
                    dic[t].append(i.split()[0])
            
            print dic
        return dict(tokens=tokens,error=error,tables=tables,test = request.get_vars,dic=dic)
    except:
        print "weee"
        error = "failed to connect to MySQL: "
        return dict(tokens=tokens,error=error,tables=tables,dic=dic)

    return dict(message="hello from hdfstocassandra.py")


@auth.requires_login()
def mysqltocassandra():
    tables = [];
    tables1 = [];
    path = '/'
    error = ""
    if 'path' in request.get_vars:
        path = request.get_vars['path'];

    try :
        connection = MySQLdb.connect(host='localhost',user='root', passwd='hadoop', db=database_name)
        cur=connection.cursor()
        query = "show tables"
        cur.execute(query)
        tables1 = cur.fetchall()
        print tables1
        dic1 ={}
        for (t,) in tables1:
            print t;
            cur = connection.cursor()
            query = "desc "+t +";";
            cur.execute(query);
            columns = cur.fetchall()
            dic1[t] = []
            for i in columns:
                    dic1[t].append(i[0])
        print dic1
#                    print dic[t]
    except:
        print "weee473"
        error = "failed to connect to MySQL: "

    try :
        b = """ cqlsh -e " use hadoop;describe tables " """
        print b;
        output = os.popen(b).read()
        print "output is "
        print output
        tables = output.split();
        print tables
        dic ={}
        for t in tables:
            query = """ cqlsh -e " use hadoop;describe columnfamily """ + t + """ " """;
            output = os.popen(query).read()
            output = output.split(',')
            output[0] = output[0].split('(')[1]
            print output
            dic[t] = []

            for i in output:
                if len(i.split()) == 2:
                    print i.split()[0]
                    dic[t].append(i.split()[0])
            print dic
#                    print dic[t]
        print "do something for our country"
        print tables
        print request.get_vars
        print dic
        return dict(error=error,tables=tables,tables1=tables1,test = request.get_vars,dic=dic,dic1=dic1)
#        return dict(tables=tables,test = request.get_vars,dic=dic)
    except:
        print "weee"
        error = "failed to connect to MySQL: "
        tables = []
        tables1 = []
        dic1 = {}
        dic = {}
        return dict(error=error,tables=tables,tables1=tables1,test = request.get_vars,dic=dic,dic1=dic1)
#        return dict(tokens=tokens,error=error,tables=tables,dic=dic)

    return dict(message="hello from hdfstocassandra.py")

@auth.requires_login()
def cassandratomysql():
    tables = [];
    tables1 = [];
    path = '/'
    if 'path' in request.get_vars:
        path = request.get_vars['path'];
    error = ""
    try :
        connection = MySQLdb.connect(host='localhost',user='root', passwd='hadoop', db=database_name)
        cur=connection.cursor()
        query = "show tables"
        cur.execute(query)
        tables1 = cur.fetchall()
        print tables1
        dic1 ={}
        for (t,) in tables1:
            print t;
            cur = connection.cursor()
            query = "desc "+t +";";
            cur.execute(query);
            columns = cur.fetchall()
            dic1[t] = []
            for i in columns:
                    dic1[t].append(i[0])
        print dic1
#                    print dic[t]
    except:
        print "weee473"
        error = "failed to connect to MySQL: "

    try :
        b = """ cqlsh -e " use hadoop;describe tables " """
        print b;
        output = os.popen(b).read()
        print "output is "
        print output
        tables = output.split();
        print tables
        dic ={}
        for t in tables:
            query = """ cqlsh -e " use hadoop;describe columnfamily """ + t + """ " """;
            output = os.popen(query).read()
            output = output.split(',')
            output[0] = output[0].split('(')[1]
            print output
            dic[t] = []

            for i in output:
                if len(i.split()) == 2:
                    print i.split()[0]
                    dic[t].append(i.split()[0])
            print dic
#                    print dic[t]
        print "do something for our country"
        print tables
        print request.get_vars
        print dic
        return dict(error=error,tables=tables,tables1=tables1,test = request.get_vars,dic=dic,dic1=dic1)
#        return dict(tables=tables,test = request.get_vars,dic=dic)
    except:
        print "weee"
        error = "failed to connect to MySQL: "
        tables = []
        tables1 = []
        dic1 = {}
        dic = {}
        return dict(error=error,tables=tables,tables1=tables1,test = request.get_vars,dic=dic,dic1=dic1)
#        return dict(tokens=tokens,error=error,tables=tables,dic=dic)

    return dict(message="hello from hdfstocassandra.py")


@auth.requires_login()
def mysqltocassandra2():
    global is_hdfs_to_cassandra;
    global hdfs_cass_posvars;
    if is_hdfs_to_cassandra == 1:
        posvars = hdfs_cass_posvars;
        is_hdfs_to_cassandra = 0
        print "Jai Guru Dev"
    else:
        posvars=request.post_vars;
    quotes = """ " """
    quotes = quotes.split()[0]
    comm = "echo hadoop | sudo -S rm /tmp/data.txt; mysql --user=root --password=hadoop -e "+quotes+quotes+quotes+ """ use hadoop;SELECT * from """ + posvars['targetdir'] +""" INTO OUTFILE '/tmp/data.txt' fields terminated by ','; """ +quotes+quotes+quotes+""" ; echo hadoop | sudo -S mv /tmp/data.txt /home/hduser/. """;
    print comm;
    result = os.popen(comm).read();
    print "the result is           ",
    print result

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
        #mysql --user=root --password=hadoop -e """ LOAD DATA INFILE 'data.txt' INTO TABLE hadoop.inputs FIELDS TERMINATED BY ','; """
        
        comm= """ cqlsh -e " use hadoop; copy """ + posvars['table'] + """ from '/home/hduser/data.txt' " """

#        comm= """ cqlsh -e " use hadoop; copy"""+ posvars['table1'] +"""(id,email,org_id,password,status,username) from '/home/hduser/data.txt' " """
        os.popen(comm).read()    #""" cqlsh -e " use hadoop; copy login(id,email,org_id,password,status,username) from '/home/hduser/data.txt' " """).read()
#        os.execlp('bash',str(present_pid));
#            os.execlp('bash','bash',str(present_pid));

@auth.requires_login()
def cassandratomysql2():

    #cassandra to csv file
    print "lord Vamsi is going to transfer the data to cassandra......"
    posvars=request.post_vars;
    comm = """ cqlsh -e " copy hadoop."""+posvars['table']+""" to 'temp.csv' " """
    print comm
    result = os.popen(comm).read()

    #csv file to mysql
    quotes = """ " """
    quotes = quotes.split()[0]
    comm = "echo hadoop | sudo -S  mv temp.csv /var/lib/mysql/hadoop/.;  mysql --user=root --password=hadoop -e " +quotes+quotes+quotes+ """ LOAD DATA INFILE 'temp.csv' INTO TABLE hadoop.""" + posvars['targetdir'] +""" FIELDS TERMINATED BY ','; """ +quotes+quotes+quotes;
    print comm
    result = os.popen(comm).read();
