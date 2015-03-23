#include "RelOp.h"
#include <time.h>

void *Run_SelectFile(void *arg_in){
	cout<<"Run_SelectFile"<<endl;
	thread_args_SelectFile *arg = (thread_args_SelectFile *)arg_in;
	//Record *to_push = new Record();
	Record *to_push;
	arg->inFile->MoveFirst();
	while(arg->inFile->GetNext(*to_push,*arg->selOp,*arg->literal) == 1){
		arg->outPipe->Insert(to_push);
	}
	arg->outPipe->ShutDown();
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
///////////////////////////////////////////////////////////////////////////////////////

void *Run_SelectPipe(void *arg_in){
	cout<<"Run_SelectPipe"<<endl;
	thread_args_SelectPipe *arg = (thread_args_SelectPipe *)arg_in;
	//Record *to_push = new Record();
	Record *to_push = new Record();
	ComparisonEngine* comp = new ComparisonEngine();
	//arg->inFile->MoveFirst();
	while(arg->inPipe->Remove(to_push) == 1){
		if (comp->Compare (to_push, arg->literal, arg->selOp)){
			arg->outPipe->Insert(to_push);
		}
	}
	arg->outPipe->ShutDown();
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { 
	thread_args_SelectPipe t_args = {&inPipe, &outPipe, &selOp, &literal};
    pthread_create(&thread,NULL,Run_SelectPipe,(void *)&t_args);
    return;
}

void SelectPipe::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void SelectPipe::Use_n_Pages (int n) { 

}
///////////////////////////////////////////////////////////////////////////////////////////

void *Run_Project(void *arg_in){
	cout<<"Run_Project"<<endl;
	thread_args_Project *arg = (thread_args_Project *)arg_in;
	//Record *to_push = new Record();
	Record *to_push = new Record();
	
	//arg->inFile->MoveFirst();
	while(arg->inPipe->Remove(to_push) == 1){
		to_push->Project(arg->keepMe, arg->numAttsOutput, arg->numAttsInput);
		arg->outPipe->Insert(to_push);	
	}
	arg->outPipe->ShutDown();
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { 
	thread_args_Project t_args = {&inPipe, &outPipe, keepMe, numAttsInput, numAttsOutput};
    pthread_create(&thread,NULL,Run_Project,(void *)&t_args);
}
	
void Project::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void Project::Use_n_Pages (int n) { 

}
////////////////////////////////////////////////////////////////////////////////////////////

void *Run_Join(void *arg_in){
	cout<<"Run_Join"<<endl;
	thread_args_Join *arg = (thread_args_Join *)arg_in;
	Record *left = new Record();
	Record *right = new Record();
	//Record dummy;
	Pipe sorted_output_l(100);
	Pipe sorted_output_r(100);
	OrderMaker sort_order_right;
	OrderMaker sort_order_left;
	int order_possible;
	int loop_var = 1;
	ComparisonEngine ce;
	order_possible = arg->selOp->GetSortOrders(sort_order_left, sort_order_right); // is 0 when ordering is not possible so do cross product else sort merge;
	if(order_possible != 0){
		// do sort merge
		cout<<"Sort Merge possible in join    RelOp.cc"<<endl;
		BigQ bq_left(*arg->inPipeL, sorted_output_l, sort_order_left, run_length);
		BigQ bq_right(*arg->inPipeR, sorted_output_r, sort_order_right, run_length);
		if(sorted_output_l.Remove(left) == 0){
			cout<<"Left Pipe is empty"<<endl;
			loop_var = 0;
		}
		if(sorted_output_r.Remove(right) == 0){
			cout<<"Right Pipe is empty"<<endl;
			loop_var = 0;
		}
		int *left_bits = (int *)left->bits;
		int *right_bits = (int *)right->bits;
		int num_atts_left = (left_bits[1] - 1)/4;
		int num_atts_right = (right_bits[1] - 1)/4;
		cout << "num_atts_left = "<<num_atts_left<<"	num_atts_right = "<<num_atts_right<<endl;
		int num_atts_to_keep = num_atts_left + num_atts_right;
		int start_of_right = num_atts_left;
		int atts_to_keep[num_atts_to_keep];
		for(int i=0;i<num_atts_left;i++)
			atts_to_keep[i] = i;
		for(int i = num_atts_left;i<num_atts_to_keep;i++)
			atts_to_keep[i] = i;

	//	void MergeRecords (Record *left, Record *right, int numAttsLeft, int numAttsRight, int *attsToKeep, int numAttsToKeep, int startOfRight);


		while(loop_var){
			int comp_val = ce.Compare(left,&sort_order_left,right,&sort_order_right);
			if(comp_val == 0){
				// merge
				Record dummy;
				dummy.MergeRecords(left,right,num_atts_left,num_atts_right,atts_to_keep,num_atts_to_keep,start_of_right);
				arg->outPipe->Insert(&dummy);
				if(sorted_output_l.Remove(left) == 0){
					cout<<"Left Pipe is empty"<<endl;
					loop_var = 0;
				}
				if(sorted_output_r.Remove(right) == 0){
					cout<<"Right Pipe is empty"<<endl;
					loop_var = 0;
				}
			}
			else if(comp_val < 0){
				//left is smaller; advance left;
				if(sorted_output_l.Remove(left) == 0){
					cout<<"Left Pipe is empty"<<endl;
					loop_var = 0;
				}
			}
			else if(comp_val > 0){
				// left is larger; advance right;
				if(sorted_output_r.Remove(right) == 0){
					cout<<"Right Pipe is empty"<<endl;
					loop_var = 0;
				}
			}
			else{
				cout<<"Error in Join  RelOp.cc"<<endl;
			}
		}
		cout<<"Sort Merge in Join done     RelOp.cc"<<endl;
	}
	else{
		cout<<"Doing Cross Product in Join    RelOp.cc"<<endl;
		// do cross product
		// put everything from right input pipe into dbfile;
		// nameing db file with random name;
		srand (time(NULL));
		char temp_file_name[10];
		temp_file_name[9] = '\0';
		temp_file_name[0] = 't';
		temp_file_name[1] = 'e';
		temp_file_name[2] = 'm';
		temp_file_name[3] = 'p';
		int extract_info_flag = 1;
		for(int i=4;i<9;i++){
			temp_file_name[i] = 'a' + rand()%20;
		}

		int num_atts_left;
		int num_atts_right;
		//cout << "num_atts_left = "<<num_atts_left<<"	num_atts_right = "<<num_atts_right<<endl;
		int num_atts_to_keep;
		int start_of_right;
		int *atts_to_keep;

		DBFile temp_file;
		temp_file.Create(temp_file_name, heap, NULL);
		while(arg->inPipeR->Remove(right)){
			temp_file.Add(*right);
		}
		temp_file.MoveFirst();
		while(arg->inPipeL->Remove(left)){
			Record *temp_merge = new Record();
			while(temp_file.GetNext(*temp_merge)){
				
				if(extract_info_flag == 1){
					int *left_bits = (int *)left->bits;
					int *right_bits = (int *)temp_merge->bits;
					num_atts_left = (left_bits[1] - 1)/4;
					num_atts_right = (right_bits[1] - 1)/4;
					cout << "num_atts_left = "<<num_atts_left<<"	num_atts_right = "<<num_atts_right<<endl;
					num_atts_to_keep = num_atts_left + num_atts_right;
					start_of_right = num_atts_left;
					atts_to_keep = new int[num_atts_to_keep];
					for(int i=0;i<num_atts_left;i++)
						atts_to_keep[i] = i;
					for(int i = num_atts_left;i<num_atts_to_keep;i++)
						atts_to_keep[i] = i;
					extract_info_flag = 0; 
				}

				Record to_output_pipe;
				to_output_pipe.MergeRecords(left, temp_merge, num_atts_left, num_atts_right, atts_to_keep, num_atts_to_keep, start_of_right);
				arg->outPipe->Insert(&to_output_pipe);
			}
			temp_file.MoveFirst();
		}
		cout<<"Cross Product Join done    RelOp.cc"<<endl;

	}	
	
	arg->outPipe->ShutDown();
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
	thread_args_Join t_args = {&inPipeL, &inPipeR, &outPipe, &selOp, &literal};
    pthread_create(&thread,NULL,Run_Join,(void *)&t_args);
}

void Join::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void Join::Use_n_Pages (int n) { 
	run_length = n;
}

///////////////////////////////////////////////////////////////////////////////////////////
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 

}

void DuplicateRemoval::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void DuplicateRemoval::Use_n_Pages (int n) { 

}

///////////////////////////////////////////////////////////////////////////////////////////
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 

}

void Sum::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void Sum::Use_n_Pages (int n) { 

}

////////////////////////////////////////////////////////////////////////////////////////////
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { 

}

void GroupBy::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void GroupBy::Use_n_Pages (int n) { 

}

/////////////////////////////////////////////////////////////////////////////////////////////
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 

}

void WriteOut::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void WriteOut::Use_n_Pages (int n) { 

}
