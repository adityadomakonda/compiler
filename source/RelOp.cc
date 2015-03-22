#include "RelOp.h"

void *Run_SelectFile(void *arg){
	cout<<"Run_SelectFile"<<endl;
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {

    thread_args_SelectFile t_args = {&inFile, &outPipe, &selOp, &literal};
    pthread_create(&thread,NULL,Run_SelectFile,(void *)&t_args);
    return;
}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
	return;
}

void SelectFile::Use_n_Pages (int runlen) {

}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { 

}

void SelectPipe::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void SelectPipe::Use_n_Pages (int n) { 

}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { 

}
	
void Project::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void Project::Use_n_Pages (int n) { 

}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 

}

void Join::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void Join::Use_n_Pages (int n) { 

}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 

}

void DuplicateRemoval::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void DuplicateRemoval::Use_n_Pages (int n) { 

}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 

}

void Sum::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void Sum::Use_n_Pages (int n) { 

}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { 

}

void GroupBy::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void Use_n_Pages (int n) { 

}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 

}

void WriteOut::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void WriteOut::Use_n_Pages (int n) { 

}
