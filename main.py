from pyparsing import Literal, CaselessLiteral, Word, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, upcaseTokens

simpleSQL = 0

class query:
    def __init__(self,s):
        self.statement = s
        print s
        f = parse()
        d = f.result(s)
        print d

class parse():
    def result(self,query):
        selectStmt = Forward()
        SELECT = Keyword("select", caseless=True)
        FROM = Keyword("from", caseless=True)
        WHERE = Keyword("where", caseless=True)

        ident          = Word( alphas, alphanums + "_$" ).setName("identifier")
        columnName     = ( delimitedList( ident, ".", combine=True ) ).setName("column name").addParseAction(upcaseTokens)
        columnNameList = Group( delimitedList( columnName ) )
        tableName      = ( delimitedList( ident, ".", combine=True ) ).setName("table name").addParseAction(upcaseTokens)
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
        whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereExpression ) 

        # define the grammar
        selectStmt <<= (SELECT + ('*' | columnNameList)("columns") + 
                        FROM + tableNameList( "tables" ) + 
                        Optional(Group(WHERE + whereExpression), "")("where"))

        simpleSQL = selectStmt
        parsedstring = simpleSQL.parseString(query)
        return parsedstring


q = query("SELECT * from XYZZY, ABC")
