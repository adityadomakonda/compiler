
CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

test3: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test3.o
	$(CC) -o bin/test3 Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test3.o -lfl -lpthread

test2: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test2.o
	$(CC) -o bin/test2 Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test2.o -lfl -lpthread
	
test1: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test1.o
	$(CC) -o bin/test1 Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test1.o -lfl -lpthread

a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl

test3.o: source/test3.cc
	$(CC) -g -c source/test3.cc

test1.o: source/test1.cc
	$(CC) -g -c source/test1.cc
	
test2.o: source/test2.cc
	$(CC) -g -c source/test2.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

Comparison.o: source/Comparison.cc
	$(CC) -g -c source/Comparison.cc
	
ComparisonEngine.o: source/ComparisonEngine.cc
	$(CC) -g -c source/ComparisonEngine.cc
	
Pipe.o: source/Pipe.cc
	$(CC) -g -c source/Pipe.cc

BigQ.o: source/BigQ.cc
	$(CC) -g -c source/BigQ.cc

DBFile.o: source/DBFile.cc
	$(CC) -g -c source/DBFile.cc

RelOp.o: source/RelOp.cc
	$(CC) -g -c source/RelOp.cc

Function.o: source/Function.cc
	$(CC) -g -c source/Function.cc

File.o: source/File.cc
	$(CC) -g -c source/File.cc

Record.o: source/Record.cc
	$(CC) -g -c source/Record.cc

Schema.o: source/Schema.cc
	$(CC) -g -c source/Schema.cc
	
y.tab.o: source/Parser.y
	yacc -d source/Parser.y
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

yyfunc.tab.o: source/ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d source/ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c

lex.yy.o: source/Lexer.l
	lex  source/Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: source/LexerFunc.l
	lex -Pyyfunc source/LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
