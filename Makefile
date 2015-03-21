
CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

test2: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test2.o
	$(CC) -o bin/test2 Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test2.o -lfl -lpthread
	
test1: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test1.o
	$(CC) -o bin/test1 Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test1.o -lfl -lpthread

a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl

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

File.o: source/File.cc
	$(CC) -g -c source/File.cc

Record.o: source/Record.cc
	$(CC) -g -c source/Record.cc

Schema.o: source/Schema.cc
	$(CC) -g -c source/Schema.cc
	
y.tab.o: source/Parser.y
	yacc -d source/Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: source/Lexer.l
	lex  source/Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
