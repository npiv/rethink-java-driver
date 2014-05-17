import re

#classes = []
#for line in open("ast.py","r"):
    #m = re.search("class (.*?)\((.*)\)", line)
    #if m:
        #print m.group(1), m.group(2)

lines= open("ast.py","r").readlines()
i= 0
while i < len(lines):
    m = re.search("class (.*?)\((.*)\)", lines[i])
    if m and re.search("tt = ", lines[i+1]):
        name, typ, tt = m.group(1), m.group(2), lines[i+1].split(".")[-1][0:-1]

        if name in ("Map","Object"):
            name = "R%s" % name

        if name in ("DB", "Datum", "Table", "Func", "Http", "MakeObj"):
            i+=1
            continue

        open("%s.java"%name,"w").write("""
        package com.rethinkdb.ast.query.gen;

        import com.rethinkdb.proto.Q2L;
        import com.rethinkdb.ast.query.RqlQuery;

        import java.util.*;

        // extends %s
        public class %s extends RqlQuery {

            public %s(List<Object> args, java.util.Map<String, Object> optionalArgs) {
                this(null, args, optionalArgs);
            }

            public %s(RqlQuery prev, List<Object> args, Map<String, Object> optionalArgs) {
                super(prev, Q2L.Term.TermType.%s, args, optionalArgs);
            }
        }
        """ % (typ,name,name,name,tt))
    i+=1

