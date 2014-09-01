# coding: utf8
# try something like
@auth.requires_login()
def index():
    tables = [];
    path = '/'
    print "i'm in vamsi's"
    if 'path' in request.get_vars:
        path = request.get_vars['path'];
    a = "hadoop fs -ls "+path+" | tr -s ' ' | cut -d ' ' -f8"
    output = os.popen(a).read()
    tokens = output.split('\n')
    error = ""
    try :
        b = """ cqlsh -e " use hadop;describe tables " """
        print b;
        output = os.popen(b).read()
        print "output is "
        print output
        tables = output.split('\n');
        print tables
        dic ={}
        for t in tables:
            query = """ cqlsh -e " use hadoop;describe columnfamily """ + i + """ " """;
            output = os.popen(query).read()
            print output
            output = output.split(',')
            output[0] = output[0].split('(')[1]

            for i in output:
                if len(i.split(' ')) == 4:
                    dic[t].append(i.split(' ')[2])
            print dic
#                    print dic[t]
        return dict(tokens=tokens,error=error,tables=tables,test = request.get_vars,dic=dic)
    except:
        print "weee"
        error = "failed to connect to MySQL: "
        return dict(tokens=tokens,error=error,tables=tables,dic=dic)

    return dict(message="hello from hdfstocassandra.py")
