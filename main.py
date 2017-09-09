from pyparsing import Literal, CaselessLiteral, Word, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, upcaseTokens

class query:
    def __init__(self,s):
        self.statement = s
        print "Query:",s
        
        newinput = parse(self) # New instance of Parse
        output = newinput.result(s)
        newinput.separatequery(output,newinput)
         
        print


class parse(query):
    def __init__(self,parent):
        self.parent = parent

    def separatequery(self,line,inp):
        self.parent.table1 = line.tables[0]
        
        self.parent.table2_exists = 0
        if(len(line.tables)>1):
            self.parent.table2_exists = 1
            self.parent.table2 = line.tables[1]

        self.parent.printcollist = line.columns

        self.parent.andFlag = 0
        self.parent.orFlag = 0

        if((len(line.where[0])>1)):
            if(line.where[0][2]=='and'):
                self.parent.andFlag = 1
            elif(line.where[0][2]=='or'):
                self.parent.orFlag = 1

        self.parent.dict1 , self.parent.dict2 = inp.directcreate()
        self.parent.table1_where,self.parent.table2_where,self.parent.table_double_where = inp.dictry(line.where[0])

        self.parent.table1_print_attributes = []
        self.parent.table2_print_attributes = []
        
        for i in self.parent.printcollist:
            if '.' not in i:
                if i in self.parent.dict1.keys():
                    self.parent.table1_print_attributes.append(int(self.parent.dict1[i]))
                elif i in self.parent.dict2.keys():
                    self.parent.table2_print_attributes.append(int(self.parent.dict2[i]))

            else:
                splittedstring = i.split('.')
                x = splittedstring[1]
                if(splittedstring[0] == self.parent.table1):
                    self.parent.table1_print_attributes.append(int(self.parent.dict1[x]))
                elif(table2_exists==1):
                    if(splittedstring[0] == self.parent.table2):
                        self.parent.table2_print_attributes.append(int(self.parent.dict2[x]))

        print self.parent.table1_print_attributes
        print self.parent.table2_print_attributes    

    def directcreate(self):
        dict1 = {}
        dict2 = {}
        f=open("metadata.txt","r")

        for i in f:
            if self.parent.table1 in i:
                index=1
                for c in f:
                    if "end" in c:
                        break
                    else:
                        dict1[c.strip()]=index
                        index = index+1

            if (self.parent.table2_exists):
                if(self.parent.table2 in i):
                    index=1
                    for c in f:
                        if "end" in c:
                            break
                        else:
                            dict2[c.strip()]=index
                            index = index+1
        f.close()
        print "Dictionary 1",dict1
        print "Dictionary 2",dict2
        return dict1 , dict2

    def dictry(self,line):
        
        list1 = []
        list2 = []
        list3 = []
        dict3 = {}
        count1 = 0
        count2 = 0
        if(len(line)>1):
            print "line:",line
            word1 = line[1]
            print "keyword:",word1
            if word1[0] in self.parent.dict1:
                dict3["attr1_index"] = self.parent.dict1[word1[0]]
                count1 = count1 + 1
            elif word1[0] in self.parent.dict2:
                dict3["attr1_index"] = self.parent.dict2[word1[0]]
                count2 = count2 + 1
            dict3["operator"] = word1[1]
            dict3["flag"] = True # 2nd is attribute
            if word1[2] in self.parent.dict1:
                dict3["second_val"] = self.parent.dict1[word1[2]]
                count1 = count1 + 1
            elif word1[2] in self.parent.dict2:
                dict3["second_val"] = self.parent.dict2[word1[2]]
                count2 = count2 + 1
            else:
                dict3["flag"] = False
                dict3["second_val"] = word1[2]
            print "Dictionary 3:",dict3

        if(count2==0):
            list1.append(dict3)
        elif(count1==0):
            list2.append(dict3)
        else:
            list3.append(dict3)
        count1=0
        count2=0
        dict4 = {}
        if(len(line)>3):
            word1 = line[3]
            print "keyword:",word1
            if word1[0] in self.parent.dict1:
                dict4["attr1_index"] = self.parent.dict1[word1[0]]
                count1 = count1 + 1
            elif word1[0] in self.parent.dict2:
                dict4["attr1_index"] = self.parent.dict2[word1[0]]
                count2 = count2 + 1
            dict4["operator"] = word1[1]
            dict4["flag"] = True # 2nd is attribute
            if word1[2] in self.parent.dict1:
                dict4["second_val"] = self.parent.dict1[word1[2]]
                count1 = count1 + 1
            elif word1[2] in self.parent.dict2:
                dict4["second_val"] = self.parent.dict2[word1[2]]
                count2 = count2 + 1
            else:
                dict4["flag"] = False
                dict4["second_val"] = word1[2]
            print "Dictionary 4:",dict4
        if(count2==0):
            list1.append(dict4)
        elif(count1==0):
            list2.append(dict4)
        else:
            list3.append(dict4)

        print list1
        print list2
        print list3
        return list1,list2,list3
    
    def result(self,line):
        selectStmt = Forward()
        SELECT = Keyword("select", caseless=True)
        FROM = Keyword("from", caseless=True)
        WHERE = Keyword("where", caseless=True)

        ident          = Word(alphas, alphanums + "_$" ).setName("identifier")
        columnName     = ( delimitedList( ident, ".", combine=True ) ).setName("column name")
        columnNameList = Group( delimitedList( columnName ) )
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
                        Optional(Group(WHERE + whereExpression), "")("where"))

        simpleSQL = selectStmt
        parsedstring = simpleSQL.parseString(line)
        return parsedstring

q = query("SELECT table1.A,C,D from table1,table2 WHERE A = '5' AND D = '6'")
#z = query("SELECT * from table1")
