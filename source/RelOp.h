#ifndef REL_OP_H
#define REL_OP_H

#include <pthread.h>
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"


class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
	pthread_t thread;
	DBFile *input;
	Pipe *output;
	CNF *cnf;
	Record *record;
	static void * callinThread(void *arg);
	// Record *buffer;

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
	private:
	pthread_t thread;
	Pipe *input;
	Pipe *output;
	CNF *cnf;
	Record *record;
	static void * callinThread(void *arg);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Project : public RelationalOp {
	private:
	pthread_t thread;
	Pipe *input;
	Pipe *output;
	int* attrOrder;
	int InputAtts;
	int OutputAtts;
	static void * callinThread(void *arg);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class Join : public RelationalOp { 
	private:
	pthread_t JoinThread;

	public:
	Pipe *inPLeft, *inPRight, *outP;
	CNF *selectionOp;
	Record *lit;
	int numOfPages;
	static void* JoinThreadProcess(void* arg);

	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

/*
class Join : public RelationalOp {
	private :
		Pipe *inPipeL;
		Pipe *inPipeR;
		Pipe *outPipe;
		CNF  *selOP;
		Record *literal;
		int runlen;
		pthread_t thread;
		static void* workerFunc( void * );
		static void nestedJoin( void * );
		static void sortMergeJoin( void *, OrderMaker &leftO, OrderMaker &rightO );
	public:
		void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};
*/
class DuplicateRemoval : public RelationalOp {
	private:
	pthread_t thread;	
	Pipe *input;
	Pipe *output;
	Schema *schema;
	static void * callinThread(void *arg);
	int runlength = 10;	
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Sum : public RelationalOp {
	private:
	pthread_t thread;
	Pipe *input;
	Pipe *output;
	Function *f;
	static void * callinThread(void *arg);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class GroupBy : public RelationalOp {
	private:
	pthread_t thread;
	Pipe *input;	
	Pipe *output;
	Function *f;
	OrderMaker *om;
	static void * callinThread(void *arg);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class WriteOut : public RelationalOp {
	private:
	pthread_t thread;
	Pipe *input;	
	FILE *output;
	Schema *schema;
	static void * callinThread(void *arg);
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

#endif