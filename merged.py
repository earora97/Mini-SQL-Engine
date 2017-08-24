from pyparsing import Literal, CaselessLiteral, Word, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, upcaseTokens
import re

class query:
    def __init__(self,s):
        self.statement = s

        newinput = parse(self) # New instance of Parse
        output = newinput.result(s)
        self.aggregate_v={"min":None,"max":None,"sum":None, "avg":None}
        newinput.separatequery(output,newinput)
        self.table1_print_names=['A','C']
        self.table2_print_names=['B']
        self.aggregate_func={}
        self.distinct=[{},{}]
        self.printHeaders()
        x=data_retrieve(self)
        self.printAggregates()

    def printHeaders(self):
        row=[]
        """for i in self.table1_print_names: row.append(i);
        for i in self.table2_print_names: row.append(i);"""
        for i in self.printcollist: row.append(i)
        print(','.join(row))

    def printAggregates(self):
        for i in self.aggregate_v.keys():
            if self.aggregate_v[i] is not None:
                print(self.aggregate_v[i])



class parse(query):
    def __init__(self,parent):
        self.parent = parent

    def separatequery(self,line,inp):
        self.parent.table1 = line.tables[0]

        self.parent.table2_exists = False
        if(len(line.tables)>1):
            self.parent.table2_exists = True
            self.parent.table2 = line.tables[1]

        self.parent.printcollist = line.columns

        self.parent.andFlag = False
        self.parent.orFlag = False
        if((len(line.where)>2)):
            if(line.where[2]=='and'):
                self.parent.andFlag = True
            elif(line.where[2]=='or'):
                self.parent.orFlag = True

        self.parent.dict1 , self.parent.dict2 = inp.directcreate()
        self.parent.table1_where,self.parent.table2_where,self.parent.table_double_where = inp.dictry(line.where)

        self.parent.table1_print_attributes = []
        self.parent.table2_print_attributes = []
        
        for i in self.parent.printcollist:
            print "i:",i
            if '(' in i:
                listagg = []
                s = re.findall(r'\((.*?)\)', i)
                if s[0] in self.parent.dict1.keys():
                    listagg.append(1)
                elif s[0] in self.parent.dict2.keys():
                    listagg.append(2)
                listagg.append(s[0])
                if('min' in i):
                    self.parent.aggregate_v["min"] = listagg
                elif('max' in i):
                    self.parent.aggregate_v['max'] = listagg
                elif('avg' in i):
                    self.parent.aggregate_v['avg'] = listagg
                elif('sum' in i):
                    self.parent.aggregate_v['sum'] = listagg
                elif('distinct' in i):
                    distinctagg = []
                    d1 = {}
                    d2 = {}
                    if s[0] in self.parent.dict1.keys():
                        z = self.parent.dict1[s[0]]
                        d1[z] = None
                    elif s[0] in self.parent.dict2.keys():
                        z = self.parent.dict2[s[0]]
                        d2[1] = None
                    distinctagg.append(d1)
                    distinctagg.append(d2)

            elif '.' not in i:
                if i in self.parent.dict1.keys():
                    self.parent.table1_print_attributes.append(int(self.parent.dict1[i]))
                elif i in self.parent.dict2.keys():
                    self.parent.table2_print_attributes.append(int(self.parent.dict2[i]))

            elif '(' not in i:
                splittedstring = i.split('.')
                x = splittedstring[1]
                if(splittedstring[0] == self.parent.table1):
                    self.parent.table1_print_attributes.append(int(self.parent.dict1[x]))
                elif(self.parent.table2_exists==1):
                    if(splittedstring[0] == self.parent.table2):
                        self.parent.table2_print_attributes.append(int(self.parent.dict2[x]))

            
     
    def directcreate(self):
        dict1 = {}
        dict2 = {}
        self.parent.table1_print_attributes=[]
        self.parent.table2_print_attributes=[]
        f=open("metadata.txt","r")

        for i in f:
            if self.parent.table1 in i:
                index=1
                for c in f:
                    if "end" in c:
                        break
                    else:
                        dict1[c.strip()]=index
                        for i in self.parent.printcollist:
                            if c.strip() in i:
                                self.parent.table1_print_attributes.append(index)
                        index = index+1

            if (self.parent.table2_exists):
                if(self.parent.table2 in i):
                    index=1
                    for c in f:
                        if "end" in c:
                            break
                        else:
                            dict2[c.strip()]=index
                            for i in self.parent.printcollist:
                                if c.strip() in i:
                                    self.parent.table2_print_attributes.append(index)
                            index = index+1
        f.close()
        return dict1 , dict2

    def dictry(self,line):

        list1 = []
        list2 = []
        list3 = []
        dict3 = {}
        count1 = 0
        count2 = 0
        if(len(line)>1):
            word1 = line[1]
            if word1[0] in self.parent.dict1.keys():
                dict3["attr1_index"] = int(self.parent.dict1[word1[0]])
                count1 = count1 + 1
            elif word1[0] in self.parent.dict2.keys():
                dict3["attr1_index"] = int(self.parent.dict2[word1[0]])
                count2 = count2 + 1
            dict3["operator"] = word1[1]
            dict3["flag"] = True # 2nd is attribute
            if word1[2] in self.parent.dict1.keys():
                dict3["second_val"] = self.parent.dict1[word1[2]]
                count1 = count1 + 1
            elif word1[2] in self.parent.dict2.keys():
                dict3["second_val"] = self.parent.dict2[word1[2]]
                count2 = count2 + 1
            else:
                dict3["flag"] = False
                dict3["second_val"] = word1[2]

        if(count2==0 and count1!=0):
            list1.append(dict3)
        elif(count1==0 and count2!=0):
            list2.append(dict3)
        elif (count2!=0 and count1!=0):
            list3.append(dict3)
        count1=0
        count2=0
        dict4 = {}
        if(len(line)>3):
            word1 = line[3]
            if word1[0] in self.parent.dict1.keys():
                dict4["attr1_index"] = int(self.parent.dict1[word1[0]])
                count1 = count1 + 1
            elif word1[0] in self.parent.dict2.keys():
                dict4["attr1_index"] = int(self.parent.dict2[word1[0]])
                count2 = count2 + 1
            dict4["operator"] = word1[1]
            dict4["flag"] = True # 2nd is attribute
            if word1[2] in self.parent.dict1.keys():
                dict4["second_val"] = self.parent.dict1[word1[2]]
                count1 = count1 + 1
            elif word1[2] in self.parent.dict2.keys():
                dict4["second_val"] = self.parent.dict2[word1[2]]
                count2 = count2 + 1
            else:
                dict4["flag"] = False
                dict4["second_val"] = word1[2]


        if(count2==0 and count1!=0):
            list1.append(dict4)
        elif(count1==0 and count2!=0):
            list2.append(dict4)
        elif (count2!=0 and count1!=0):
            list3.append(dict4)

        return list1,list2,list3

    def result(self,line):        
        
        selectStmt = Forward()
        SELECT = Keyword("select", caseless=True)
        FROM = Keyword("from", caseless=True)
        WHERE = Keyword("where", caseless=True)
        
        #Error Handling for wrong spellings
        selectword = line.split(' ')[0]
        if(selectword!=SELECT):
            print "Syntax Error! Wrong Spelling of Select"
            exit()        

        fromword = line.split(' ')[2]
        if(fromword!=FROM):
            print "Syntax Error! Wrong Spelling of From"
            exit()

        ####
        ident          = Word(alphas, alphanums + "_$" + "(" + ")").setName("identifier")
        columnName     = ( delimitedList( ident, ".", combine=True ) ).setName("column name")
        columnNameList = Group( delimitedList(columnName))
        tableName      = ( delimitedList( ident, ".", combine=True ) ).setName("table name")
        tableNameList  = Group( delimitedList( tableName ) )

        whereExpression = Forward()
        and_ = Keyword("and", caseless=True)
        or_ = Keyword("or", caseless=True)
        in_ = Keyword("in", caseless=True)

        E = CaselessLiteral("E")
        binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
        arithSign = Word("+-",exact=1)
        realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
                                                                 ( "." + Word(nums) ) ) +
                    Optional( E + Optional(arithSign) + Word(nums) ) )
        intNum = Combine( Optional(arithSign) + Word( nums ) +
                    Optional( E + Optional("+") + Word(nums) ) )

        columnRval = realNum | intNum | quotedString | columnName # need to add support for alg expressions
        whereCondition = Group(
            ( columnName + binop + columnRval ) |
            ( columnName + in_ + "(" + delimitedList( columnRval ) + ")" ) |
            ( columnName + in_ + "(" + selectStmt + ")" ) |
            ( "(" + whereExpression + ")" )
            )
        whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereCondition )

        selectStmt <<= (SELECT + ('*' | columnNameList)("columns") +
                        FROM + tableNameList( "tables" ) +
                        Optional(Group(WHERE + whereExpression)("where")))

        simpleSQL = selectStmt
        parsedstring = simpleSQL.parseString(line)
        return parsedstring

class data_retrieve(query):
    def __init__(self,parent):
        self.parent=parent
        table1_file=open(self.parent.table1 +".csv")
        if self.parent.table2_exists:
            table2_file=open(self.parent.table2+".csv")
        for l1 in table1_file:
            cond1=True
            cond2=True
            if len(self.parent.table1_where)>0:
                cond1=self.check_single_where(l1,self.parent.table1_where[0]);
            if len(self.parent.table1_where)>1:
                cond2=self.check_single_where(l1,self.parent.table1_where[1]);

            if self.parent.table2_exists:
                for l2 in table2_file:
                    if len(self.parent.table2_where)>0:
                        if len(self.parent.table1_where)>0:
                            cond2=self.check_single_where(l2,self.parent.table2_where[0])
                        else:
                            cond1=self.check_single_where(l2,self.parent.table2_where[0])
                            if len(self.parent.table2_where)>1:
                                cond2=self.check_single_where(l2,self.parent.table2_where[1])
                    if len(self.parent.table_double_where)>0:
                        if len(self.parent.table1_where)>0 or len(self.parent.table2_where)>0:
                            cond2=self.check_double_where(l1,l2,self.parent.table_double_where[0])
                        else:
                            cond1=self.check_double_where(l1,l2,self.parent.table_double_where[0])
                    if len(self.parent.table_double_where)>1:
                        cond2=self.check_double_where(l1,l2,self.parent.table_double_where[1])

                    if self.parent.andFlag and cond1 and cond2:
                        if self.aggregate(l1,l2):
                            self.printlines(l1,l2)
                    if self.parent.orFlag and (cond1 or cond2):
                        if self.aggregate(l1,l2):
                            self.printlines(l1,l2)
                    if self.parent.andFlag==False and self.parent.orFlag==False and cond1:
                        if self.aggregate(l1,l2):
                            self.printlines(l1,l2)
                table2_file.seek(0)
            #if table2 doesnt exist
            else:
                if cond1 and len(self.parent.table1_where)<2:
                    if self.aggregate(l1,None):
                        self.printlines(l1,None)
                elif len(self.parent.table1_where)>1:
                    if self.parent.andFlag and cond1 and cond2:
                        if self.aggregate(l1,None):
                            self.printlines(l1,None)
                    elif self.parent.orFlag and (cond1 or cond2):
                        if self.aggregate(l1,None):
                            self.printlines(l1,None)

    def check_single_where(self,line,where_cond):
        row=[int(x.strip()) for x in line.split(',')]
        if(where_cond['flag']):
            val1=(row[where_cond["attr1_index"]-1])
            val2=(row[int(where_cond['second_val'])-1])
            operator = where_cond['operator']
            return self.checkTrue(val1,val2,operator)

        else:
            val1=(row[where_cond["attr1_index"]-1])
            val2=int(where_cond['second_val'])
            operator=where_cond['operator']
            return self.checkTrue(val1,val2,operator)

    def checkTrue(self,val1, val2, operator):
        if(operator=="="):
            return val1==val2
        if operator=='>' :
            return val1>val2
        if operator=='<' :
            return val1<val2
        if operator==">=" :
            return val1>=val2
        if operator=="<=" :
            return val1<=val2

    def check_double_where(self, line1, line2, where_cond):
        row1=[int(x.strip()) for x in line1.split(',')]
        row2=[int(x.strip()) for x in line2.split(',')]
        val1=(row1[where_cond["attr1_index"]-1])
        val2=row2[int(where_cond['second_val'])-1]
        operator=where_cond['operator']
        return self.checkTrue(val1,val2,operator)

    def printlines(self,line1,line2):
        row1=[x.strip() for x in line1.split(',')]
        if(line2!=None):
            row2=[x.strip() for x in line2.split(',')]
        row=[]
        for i in self.parent.table1_print_attributes: row.append(row1[i-1])
        if(line2!=None):
            for i in self.parent.table2_print_attributes: row.append(row2[i-1])
        if(row!=[]):
            print(','.join(row))

    def aggregate(self,line1,line2):
        if 'max' in self.parent.aggregate_func.keys():
            if self.parent.aggregate_func['max'][0]==1:
                line=[x.strip() for x in line1.split(',')]
            else:
                line=[x.strip() for x in line2.split(',')]
            if self.parent.aggregate_v["max"] is None:
                self.parent.aggregate_v["max"]=int(line[self.parent.aggregate_func['max'][1]-1])
            else:
                self.parent.aggregate_v["max"]=max(self.parent.aggregate_v["max"],int(line[self.parent.aggregate_func['max'][1]-1]))
        if 'min' in self.parent.aggregate_func.keys():
            if self.parent.aggregate_func['min'][0]==1:
                line=[x.strip() for x in line1.split(',')]
            else:
                line=[x.strip() for x in line2.split(',')]
            if self.parent.aggregate_v["min"] is None:
                self.parent.aggregate_v["min"]=int(line[self.parent.aggregate_func['min'][1]-1])
            else:
                self.parent.aggregate_v["min"]=min(self.parent.aggregate_v["min"],int(line[self.parent.aggregate_func['min'][1]-1]))
        if 'sum' in self.parent.aggregate_func.keys():
            if self.parent.aggregate_func['sum'][0]==1:
                line=[x.strip() for x in line1.split(',')]
            else:
                line=[x.strip() for x in line2.split(',')]
            if self.parent.aggregate_v["sum"] is None:
                self.parent.aggregate_v["sum"]=int(line[self.parent.aggregate_func['sum'][1]-1])
            else:
                self.parent.aggregate_v["sum"]=self.parent.aggregate_v["sum"]+int(line[self.parent.aggregate_func['sum'][1]-1])
        if 'avg' in self.parent.aggregate_func.keys():
            if self.parent.aggregate_func['avg'][0]==1:
                line=[x.strip() for x in line1.split(',')]
            else:
                line=[x.strip() for x in line2.split(',')]
            if self.parent.aggregate_v["avg"] is None:
                self.parent.aggregate_v["avg"]=int(line[self.parent.aggregate_func['avg'][1]-1])
                self.n=1
            else:
                self.parent.aggregate_v["avg"]=((self.parent.aggregate_v["avg"])*(self.n)+int(line[self.parent.aggregate_func['avg'][1]-1]))/(self.n+1)
                self.n=self.n+1

        if 'distinct' in self.parent.aggregate_func.keys():
            for t,col in self.parent.aggregate_func['distinct']:
                if t==1:
                    line=[x.strip() for x in line1.split(',')]
                else:
                    line=[x.strip() for x in line2.split(',')]
                if col not in self.parent.distinct[t-1].keys():
                    self.parent.distinct[t-1][col]=[]
                if line[col-1] in self.parent.distinct[t-1][col]:
                    return False
                else:
                    self.parent.distinct[t-1][col].append(line[col-1])
        return True



q = query("SELECT A,distinct(D) from table1,table2 WHERE D > 15000 and C = D")
#z = query("SELECT * from table1")
