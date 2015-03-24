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
	run_length = runlen;
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
	run_length = n;
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
	run_length = n;
}
////////////////////////////////////////////////////////////////////////////////////////////

void *Run_Join(void *arg_in){
	cout<<"Run_Join"<<endl;
	thread_args_Join *arg = (thread_args_Join *)arg_in;
	Record *left = new Record();
	Record *right = new Record();
	int run_length = arg->run_length;
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
	thread_args_Join t_args = {&inPipeL, &inPipeR, &outPipe, &selOp, &literal, run_length};
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

void *Run_DuplicateRemoval(void *arg_in){
	cout<<"Run_DuplicateRemoval"<<endl;
	thread_args_DuplicateRemoval *arg = (thread_args_DuplicateRemoval *)arg_in;
	OrderMaker sort_order(arg->mySchema);
	Pipe sorted_output(100);
	int run_length = arg->run_length;
	BigQ bq(*arg->inPipe, sorted_output, sort_order, run_length);
	Record *last_added;
	last_added = new Record();
	Record *cur;
	cur = new Record();
	sorted_output.Remove(cur);
	last_added->Copy(cur);
	arg->outPipe->Insert(cur);
	ComparisonEngine ce;
	while(sorted_output.Remove(cur)){
		int comp_val = ce.Compare(last_added,cur, &sort_order);
		if(comp_val != 0){
			delete last_added;
			last_added = new Record();
			last_added->Copy(cur);
			arg->outPipe->Insert(cur);
		}
	}

	arg->outPipe->ShutDown();
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 
	thread_args_DuplicateRemoval t_args = {&inPipe, &outPipe, &mySchema, run_length};
    pthread_create(&thread,NULL,Run_DuplicateRemoval,(void *)&t_args);
}

void DuplicateRemoval::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void DuplicateRemoval::Use_n_Pages (int n) { 
	run_length = n;
}

///////////////////////////////////////////////////////////////////////////////////////////

void *Run_Sum(void *arg_in){
	cout<<"Run_Sum"<<endl;
	thread_args_Sum *arg = (thread_args_Sum *)arg_in;
	Record read_from_pipe;
  	int int_sum = 0;
  	double double_sum = 0;
  	Type   res_type = Int;
  	int int_res = 0; 
  	double double_res = 0;
  	//bool r = 0;

  	while(arg->inPipe->Remove(&read_from_pipe)){
    	res_type = arg->computeMe->Apply(read_from_pipe, int_res, double_res);
    	if(Int == res_type)
      		int_sum += int_res;
    	else if(Double == res_type)
      		double_sum += double_res;
    	else 
      		cout<<"Error in result return type from Apply 	RelOp.cc"<<endl;
  	}

  	Record *rec_to_write = new Record();
    char att_name[10];
    char schema_name[20];
    char att_val[128];

    sprintf(att_name, "sum\0");
    sprintf(schema_name, "sum_schema\0");

    Attribute att = {att_name, res_type};
    Schema sum_schema(schema_name, 1, &att);

    if(Int == res_type)
    	sprintf(att_val, "%d|\0", int_sum);
    else if(Double == res_type)
     	sprintf(att_val, "%f|\0", double_sum);

    rec_to_write->ComposeRecord(&sum_schema, att_val);
    arg->outPipe->Insert(rec_to_write);
    delete rec_to_write;

	arg->outPipe->ShutDown();
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 
	thread_args_Sum t_args = {&inPipe, &outPipe, &computeMe};
    pthread_create(&thread,NULL,Run_Sum,(void *)&t_args);
}

void Sum::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void Sum::Use_n_Pages (int n) { 
	run_length = n;
}

////////////////////////////////////////////////////////////////////////////////////////////

void composeRecordForSum(Record *sum_record,Type &res_type,int &int_sum, double &double_sum){
	//Record *sum_record = new Record();
    char att_name[10];
	char schema_name[20];
	char att_val[128];
    sprintf(att_name, "sum\0");
    sprintf(schema_name, "sum_schema\0");
    Attribute att = {att_name, res_type};
    Schema sum_schema(schema_name, 1, &att);
    if(Int == res_type)
    	sprintf(att_val, "%d|\0", int_sum);
    else if(Double == res_type)
     	sprintf(att_val, "%f|\0", double_sum);
    sum_record->ComposeRecord(&sum_schema, att_val);
    //reset values for next call
    res_type = Int;
    int_sum = 0;
    double_sum = 0;
}

void MergeRecordsToPush(Record *rec_to_push, Record *left, Record *right){
	int num_atts_left;
	int num_atts_right;
	//cout << "num_atts_left = "<<num_atts_left<<"	num_atts_right = "<<num_atts_right<<endl;
	int num_atts_to_keep;
	int start_of_right;
	int *atts_to_keep;

	int *left_bits = (int *)left->bits;
	int *right_bits = (int *)right->bits;
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

	rec_to_push->MergeRecords(left, right, num_atts_left, num_atts_right, atts_to_keep, num_atts_to_keep, start_of_right);
}

void *Run_GroupBy(void *arg_in){
	cout<<"Run_GroupBy"<<endl;
	thread_args_GroupBy *arg = (thread_args_GroupBy *)arg_in;
	
	Pipe sorted_input(100);
	//OrderMaker sort_order(arg->mySchema);
	BigQ bq(*arg->inPipe, sorted_input, *arg->groupAtts, arg->run_length);
	// Read one record and initialze variables
	Record *last_seen;
	last_seen = new Record();
	Record *cur;
	cur = new Record();
	sorted_input.Remove(cur);
	last_seen->Copy(cur);
	ComparisonEngine ce;
	// Initialize group sums
	int int_sum = 0;
  	double double_sum = 0;
  	Type   res_type = Int;
  	int int_res = 0; 
  	double double_res = 0;

	while(sorted_input.Remove(cur) == 1){
		// aggregate while same. So use sum code while same. Reset sum vars when new group is found and start over.
		int comp_val = ce.Compare(last_seen,cur, arg->groupAtts);
		if(comp_val == 0){
			// records are same according to ordermaker so compute sum
			res_type = arg->computeMe->Apply(*cur, int_res, double_res);
	    	if(Int == res_type)
	      		int_sum += int_res;
	    	else if(Double == res_type)
	      		double_sum += double_res;
	    	else 
	      		cout<<"Error in result return type from Apply 	RelOp.cc GroupBy"<<endl;
		}
		else{
			// write previous sum to record. Merge sum rec with lastseen record. Push this merged record to outPipe. Update last seen and reset sum vaiables

			Record *sum_record = new Record();
		    composeRecordForSum(sum_record, res_type, int_sum, double_sum); // Composes the new record and resets the sum the variables to 0
		    Record *rec_to_push = new Record();
		    MergeRecordsToPush(rec_to_push,sum_record,last_seen);
		    arg->outPipe->Insert(rec_to_push);
		    delete rec_to_push;
		    delete sum_record; // move this right location: Note to self
		    delete last_seen; // Update last seen
			last_seen = new Record();
			last_seen->Copy(cur);

		}
	}
	// Push the left over sum to pipe
	Record *sum_record = new Record();
    composeRecordForSum(sum_record, res_type, int_sum, double_sum); // Composes the new record and resets the sum the variables to 0		    Record *rec_to_push = new Record();
	Record *rec_to_push = new Record();
	MergeRecordsToPush(rec_to_push,sum_record,last_seen);
	arg->outPipe->Insert(rec_to_push);
	delete rec_to_push;
	delete sum_record; // move this right location: Note to self
	delete last_seen; // Update last seen


	// write previous sum to record. Merge sum rec with lastseen record. Push this merged record to outPipe
	arg->outPipe->ShutDown();
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { 
	thread_args_GroupBy t_args = {&inPipe, &outPipe, &groupAtts, &computeMe, run_length};
    pthread_create(&thread,NULL,Run_GroupBy,(void *)&t_args);
}

void GroupBy::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void GroupBy::Use_n_Pages (int n) { 
	run_length = n;
}

/////////////////////////////////////////////////////////////////////////////////////////////


void *Run_WriteOut(void *arg_in){
	cout<<"Run_WriteOut"<<endl;
	thread_args_WriteOut *arg = (thread_args_WriteOut *)arg_in;
	//Record *to_push = new Record();
	Record *to_push = new Record();
	
	while(arg->inPipe->Remove(to_push) == 1){
		to_push->PrintToFile(arg->mySchema, arg->outFile);	
	}
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 
	thread_args_WriteOut t_args = {&inPipe, outFile, &mySchema};
    pthread_create(&thread,NULL,Run_WriteOut,(void *)&t_args);
}

void WriteOut::WaitUntilDone () { 
	pthread_join (thread, NULL);
	return;
}

void WriteOut::Use_n_Pages (int n) { 
	run_length = n;
}
