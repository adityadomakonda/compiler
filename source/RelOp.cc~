#include <string>
#include <stdlib.h>
#include <string.h>
#include <sstream>
#include "RelOp.h"
#include "Function.h"

//select file
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	input = &inFile;
	output = &outPipe;
	cnf = &selOp;
	record = &literal;
	pthread_create (&thread, NULL, SelectFile::callinThread, this);
}

void * SelectFile::callinThread(void *arg){
	SelectFile *current;
	current = (SelectFile *)arg;
	Record fetchme;
	current->input->MoveFirst();
	while(current->input->GetNext(fetchme, *current->cnf, *current->record)){
	//cout << "Inside while loop before pipe" << endl;
		current->output->Insert(&fetchme);
	//cout << "Inside while loop after pipe" << endl;			
	}
	current->output->ShutDown();
}


void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}


//select pipe
void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { 
	input = &inPipe;
	output = &outPipe;
	cnf = &selOp;
	record = &literal;
	pthread_create (&thread, NULL, SelectPipe::callinThread, this);

} 

void * SelectPipe::callinThread(void *arg){
	SelectPipe *current;
	current = (SelectPipe *)arg;
	Record inRecord;
	ComparisonEngine comp;
	while(current->input->Remove(&inRecord)){
		if(comp.Compare(&inRecord,current->record,current->cnf))
			current->output->Insert(&inRecord);
	}
	current->output->ShutDown();
}

void SelectPipe::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages (int n) {
	// pthread_join (thread, NULL);
}

//Project
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	input = &inPipe;
	output = &outPipe;
	attrOrder = keepMe;
	InputAtts = numAttsInput;
	OutputAtts = numAttsOutput;
	//callinThread((void *)this);
	pthread_create (&thread, NULL, Project::callinThread, this);
}

void* Project::callinThread(void* arg){
	Project *current;
	current = (Project *)arg;
	Record inRecord;
	while(current->input->Remove(&inRecord)){
		inRecord.Project(current->attrOrder,current->OutputAtts, current->InputAtts);
		current->output->Insert(&inRecord);
	}
	current->output->ShutDown();
}

void Project::WaitUntilDone () { 
	pthread_join (thread, NULL);
}
void Project::Use_n_Pages (int n) {
}

//Dublicate Removal

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
	input = &inPipe;
	output = &outPipe;
	schema = &mySchema;
	pthread_create (&thread, NULL, DuplicateRemoval::callinThread, this);
	
}

void* DuplicateRemoval::callinThread(void* arg){
	DuplicateRemoval *current;
	current = (DuplicateRemoval *)arg;
	OrderMaker *order = new OrderMaker(current->schema);
	//buffersize 100
	Pipe temp(100);
	BigQ *bq = new BigQ(*current->input,temp,*order,current->runlength);
	Record *inRecord = new Record();
	ComparisonEngine compare;
	Record *previous = new Record();
	//check for no records in Pipe
	temp.Remove(previous);
	inRecord->Copy(previous);
	current->output->Insert(previous);
	previous->Copy(inRecord);
	while(temp.Remove(inRecord)){
		 if(compare.Compare(previous,inRecord,order)!=0){
			previous->Copy(inRecord);
			current->output->Insert(inRecord);
		}
	}
	//temp.ShutDown();
	current->output->ShutDown();
}

void DuplicateRemoval::WaitUntilDone (){
	pthread_join (thread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int n){
}

// Sum
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
	input = &inPipe;
	output = &outPipe;
	f = &computeMe;
	pthread_create (&thread, NULL, Sum::callinThread, this);
	
}

void* Sum::callinThread(void* arg){
	Sum *current;
	current = (Sum *)arg;
	Record inRecord;
	int num = 0;
	int numsum = 0;
	double dbl = 0;
	double dblsum = 0;
	Type type;
	Record outRecord;
	while(current->input->Remove(&inRecord)){
		type = current->f->Apply (inRecord, num, dbl);
		if(type == Int){
			numsum = numsum+num;
		}
		else if(type == Double){
			dblsum = dblsum+dbl;		
		}
	
	}
	if(type == Int){
		Attribute aInt;
		char *str = "newAttribute";
		aInt.name = str;
		aInt.myType = Int;
		stringstream ss;
		ss << numsum;
		ss << "|";
		string str1 = ss.str();
		const char* src = str1.c_str();
		Schema *s = new Schema(NULL,1,&aInt);
		outRecord.ComposeRecord (s, src); 
	}else if(type == Double){
		Attribute aInt;
		char *str = "newAttribute";
		aInt.name = str;
		aInt.myType = Double;
		stringstream ss;
		ss << dblsum;
		ss << "|";
		string str1 = ss.str();
		const char* src = str1.c_str();
		Schema *s = new Schema("temp",1,&aInt);
		outRecord.ComposeRecord (s, src);
		//outRecord.Print(s);	
	}
	current->output->Insert(&outRecord);
	current->output->ShutDown();
}

void Sum::WaitUntilDone (){
	pthread_join (thread, NULL);
}

void Sum::Use_n_Pages (int n){
}

//GroupBy
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
	input = &inPipe;
	output = &outPipe;
	f = &computeMe;
	om = &groupAtts;
	pthread_create (&thread, NULL, GroupBy::callinThread, this);
}

void* GroupBy::callinThread(void* arg){
	GroupBy *current;
	current = (GroupBy *)arg;
	Pipe temp(100);
	BigQ *bq = new BigQ(*current->input,temp,*current->om,10);
	Record *inRecord = new Record();

	ComparisonEngine compare;
	Record *previous = new Record();
	int result = temp.Remove(inRecord);
	Type type;
	int num = 0;
	int numsum = 0;
	double dbl = 0;
	double dblsum = 0;
	Record outRecord;
	while(1){
		if(result == 0) break;

		previous->Copy(inRecord);
		type = current->f->Apply(*inRecord, num, dbl);
		if(type == Int){
			numsum = numsum+num;
		}
		else if(type == Double){
			dblsum = dblsum+dbl;		
		}
		result = temp.Remove(inRecord); 
					
		while(result==1 && compare.Compare(previous,inRecord,current->om)==0){
			type = current->f->Apply(*inRecord, num, dbl);
			if(type == Int){
				numsum = numsum+num;
			}
			else if(type == Double){
				dblsum = dblsum+dbl;		
			}
			result = temp.Remove(inRecord); 	
		}

		if(type == Int){
		Attribute aInt;
		char *str = "newAttribute";
		aInt.name = str;
		aInt.myType = Int;
		stringstream ss;
		ss << numsum;
		ss << "|";
		string str1 = ss.str();
		const char* src = str1.c_str();
		Schema *s = new Schema(NULL,1,&aInt);
		outRecord.ComposeRecord (s, src); 
		}else if(type == Double){
		Attribute aInt;
		char *str = "newAttribute";
		aInt.name = str;
		aInt.myType = Double;
		stringstream ss;
		ss << dblsum;
		ss << "|";
		string str1 = ss.str();
		const char* src = str1.c_str();
		Schema *s = new Schema("temp",1,&aInt);
		outRecord.ComposeRecord (s, src);
		}
		current->output->Insert(&outRecord);
		numsum = 0; dblsum = 0;
	}
	current->output->ShutDown();	
		
}

void GroupBy::WaitUntilDone () {
}

void GroupBy::Use_n_Pages (int n) { 
}

void Join :: Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) 
{
	inPLeft = &inPipeL;
	inPRight = &inPipeR;
	outP = &outPipe;
	selectionOp = &selOp;
	lit = &literal;
	numOfPages = 100;

	if(pthread_create(&JoinThread, NULL, Join::JoinThreadProcess,(void *) this) != 0)
	{ 
		cerr << "Error in pthread creation" << endl; 
		return;
	}
}

void Join :: WaitUntilDone () 
{ 
	pthread_join(JoinThread, NULL);
}
 
void Join :: Use_n_Pages (int n) 
{
	numOfPages = n; 
}

void* Join::JoinThreadProcess(void* arg)
{
	cout << "Inside join\n";
	Join *join = (Join *)arg;
	ComparisonEngine comp;
	Record *leftRecord, *rightRecord,*nextleft , *nextright;
	Record *finRecord;

	OrderMaker *orderLeft = new OrderMaker();
	OrderMaker *orderRight = new OrderMaker();
	
	Pipe tmpPipeLeft(join->numOfPages);
	Pipe tmpPipeRight(join->numOfPages);

	join->selectionOp->GetSortOrders(*orderLeft, *orderRight);
	cout << "Got Sorted Orders\n";

	BigQ bigq1(*(join->inPLeft), tmpPipeLeft, *orderLeft, join->numOfPages);
	BigQ bigq2(*(join->inPRight), tmpPipeRight, *orderRight, join->numOfPages);
	cout << "Called BigQ\n";
	
	leftRecord = new Record();
	rightRecord = new Record();
	nextleft = new Record();
	nextright = new Record();
	
	tmpPipeLeft.Remove(leftRecord);
	tmpPipeRight.Remove(rightRecord);
	cout << "Removed records from both left and right\n";
	
	vector<Record*> leftsimilar,rightsimilar;
	int *attsToKeep = new int[ leftRecord->GetNumAtts() + rightRecord->GetNumAtts()];
	for(int i = 0 ; i < leftRecord->GetNumAtts();i++)
		attsToKeep[i] = i;
	for(int i = 0 ; i < rightRecord->GetNumAtts();i++)
		attsToKeep[ leftRecord->GetNumAtts()+i] = i;
	bool leftended = false , rightended = false;
/*		while(tmpPipeLeft.Remove(leftRecord) != 0) 
				{
					leftRecord = new Record();
					cout<<"inside lesser than\n";
					leftended = true;
					//break;
				}		
		while(tmpPipeRight.Remove(rightRecord) != 0) 
				{
					rightRecord = new Record();
					cout<<"inside greater than\n";
					rightended = true;
					//break;
				}
*/

	while(!(leftended||rightended))
	{
		int out = comp.Compare(leftRecord, orderLeft, rightRecord, orderRight); 
		if(out == 0)
		{
			leftsimilar.push_back(leftRecord);
			while(1)
			{
				if(tmpPipeLeft.Remove(nextleft) == 0) {
					leftended = true;
					break;
				}
				if(comp.Compare(leftRecord, nextleft, orderLeft) == 0)
				{
					leftsimilar.push_back(nextleft);
					nextleft = new Record();
				}
				else
				{
					leftRecord = nextleft;
					nextleft = new Record();
					break;
				}
			}

			rightsimilar.push_back(rightRecord);
			while(1)
			{
				if(tmpPipeRight.Remove(nextright) == 0)
				{ 
					rightended = true;
					break;
				}
				if(comp.Compare(rightRecord, nextright, orderRight) == 0)
				{
					rightsimilar.push_back(nextright);
					nextright = new Record();
				}
				else
				{
					rightRecord = nextright;
					nextright = new Record();
					break;
				}
			}

			for(int i = 0; i < leftsimilar.size(); i++)
			{
				for(int j = 0; j < rightsimilar.size(); j++)
				{
					finRecord = new Record();
					finRecord->MergeRecords(leftsimilar[i], rightsimilar[j], leftsimilar[i]->GetNumAtts(), rightsimilar[j]->GetNumAtts(), 
							attsToKeep, rightsimilar[j]->GetNumAtts() + leftsimilar[i]->GetNumAtts(), leftsimilar[i]->GetNumAtts());
					join->outP->Insert(finRecord);
				}
			}
			leftsimilar.clear();
			rightsimilar.clear();

			if(leftended || rightended)
				break;
		}
		else {
			if(out < 0)
			{
				cout<<"inside less than\n";
				if(tmpPipeLeft.Remove(leftRecord) == 0){ 
					leftended = true;
					break;
				}
			}
			else
			{
				cout<<"inside greater than\n";
				if(tmpPipeRight.Remove(rightRecord) == 0) 
				{
					rightended = true;
					break;
				}
			}
		}
	}
	cout << "End of the Function \n";
	join->outP->ShutDown();
	return NULL;
}

/*	int rightcount = 0;
	while( rightPipe.Remove( &rRecord )!=0){
		rightcount++;
	}
	cout<<"right count "<< rightcount<< "\n";
	int leftcount = 0;
	while( leftPipe.Remove( &lRecord )!=0){
		leftcount++;
	}
	cout<<"left count" << leftcount << "\n";
		
	return;*/


//WriteOut
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema){
	input = &inPipe;
	output = outFile;
	schema = &mySchema;
	pthread_create (&thread, NULL, WriteOut::callinThread, this);
}

void* WriteOut::callinThread(void* arg){
	WriteOut *current;
	current = (WriteOut *)arg;
	Record inRecord;
	while(current->input->Remove(&inRecord)){
		inRecord.PrintToFile (current->schema,current->output); 
	}
	fclose(current->output);
}

void WriteOut::WaitUntilDone (){
	pthread_join (thread, NULL);
}

void WriteOut::Use_n_Pages (int n){
}