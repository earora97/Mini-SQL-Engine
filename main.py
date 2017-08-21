from pyparsing import Literal, CaselessLiteral, Word, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, upcaseTokens

class query:
    def __init__(self,s):
        self.statement = s
        print "Query:",s
        
        newinput = parse(self)
        output = newinput.result(s)
        self.table1 = output.tables[0]
      #  print "Table1 Name:",self.table1
        
        self.table2_exists = 0
        if(len(output.tables)>1):
            self.table2_exists = 1
            self.table2 = output.tables[1]
     #   print "Table 2 Exists:", self.table2_exists

        self.printcollist = output.columns
      #  print "Columns To Be Printed:",self.printcollist

        self.andFlag = 0
        self.orFlag = 0

        if((len(output.where[0])>1)):
            if(output.where[0][2]=='and'):
                self.andFlag = 1
            elif(output.where[0][2]=='or'):
                self.orFlag = 1
        #print self.andFlag , self.orFlag

        self.dict1 , self.dict2 = newinput.directcreate()
        self.table1_where,self.table2_where,self.table_double_where = newinput.dictry(output.where[0])

      #  print "Parsed Query:",output
        print

class parse(query):
    def __init__(self,parent):
        self.parent = parent

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
            list1.append(dict3)
        elif(count1==0):
            list2.append(dict3)
        else:
            list3.append(dict3)

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

        # define the grammar
        selectStmt <<= (SELECT + ('*' | columnNameList)("columns") + 
                        FROM + tableNameList( "tables" ) + 
                        Optional(Group(WHERE + whereExpression), "")("where"))

        simpleSQL = selectStmt
        parsedstring = simpleSQL.parseString(line)
        return parsedstring

q = query("SELECT A,C,D from table1,table2 WHERE A = '5' AND D = '6'")
z = query("SELECT * from table1")
