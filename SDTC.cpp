// finalproj.cpp : This file contains the 'main' function. Program execution begins and ends there.

#include<iostream> 
#include <array>
#include<vector>
#include<fstream>
#include <sstream>
#include <cstdlib>
#include<stdlib.h>
#include <cmath>
#include<string>

using namespace std;
const  int node_num = 57590;  //number of total nodes is 57590 but disregard slaughters 3574;      // 97971;
const int total_edge = 7544956;     //6359653;
const int end_date = 1460;
const int number_of_seeds = 54135;
typedef int arraym[total_edge][4];					//define an array to keep all data based on its time
typedef unsigned int  w[node_num];
typedef  unsigned long int  w2[node_num];

w delay ;
w2  wbig, weight, barn_size;
arraym mydata;
ofstream outdata;


//***************************************the is the main function of program**********************
/*computing SDDC and SDTC for all farms with initial condition i0 and t0 */
void newmeasure(int seedid, int timepoint)              //seedid means the first infected one id=io   and timepoint=t0
{
	for (int i = 0; i < node_num; i++)					//initialize the weight list
	{
		weight[i] = barn_size[i];        //change weight from 1 to the size of barn in for weighted version
		wbig[i] = 0;

		// set defult value for delay of all nodes to infinity in my code and wbig to the total number of nodes
	}
	wbig[seedid] = weight[seedid];
	unsigned long int omega = weight[seedid];
	for (int i = 0; i < node_num; i++)
		delay[i] = 2000;                                // set defult value for delay of all nodes to infinity 2000 greater than T
	delay[seedid] = 0;
	for (int i = 0; i < total_edge; i++)               //update the value of dj for every node is possible
	{

		if ((mydata[i][2] - timepoint) >= delay[mydata[i][0]] and (mydata[i][2] - timepoint) < delay[mydata[i][1]])
		{
			delay[mydata[i][1]] = mydata[i][2] - timepoint;      //mydata[i][2] is t and mydata[i][1] is j in the algorithm
			omega = omega + weight[mydata[i][1]];
			wbig[mydata[i][1]] = omega;
		}

	}
	for (int j = 0; j < node_num; j++)                              //for uninfected nodes set wbig to the max value of omega
	{
		if (wbig[j] == 0)
			wbig[j] = omega;
	}


	return;
}
//*******************************reading data from input file*****************************
/*read_inputfile method read the movements data set and store
all in 2 dimentional array with 4 columns and set of rows */
void read_inputfile(string dataset_name)

{
	ifstream fin;
	string strPathCSVFile = dataset_name;      	// Open an existing file 
	fin.open(strPathCSVFile.c_str());
	if (!fin.is_open())
	{
		cout << "Path Wrong!!!! " << dataset_name << endl;
		exit(EXIT_FAILURE);
	}
	string line;
	string word, temp;
	int i = 0;
	for (int j = 0; j < total_edge; j++)                       //empty the array from previous dataset
		for (int k = 0; k < 4; k++)
			mydata[j][k] = 0;
	//getline(fin, line);
	while (getline(fin, line))							       // Read the Data from the file 
	{
		if (line.empty())							           // skip empty lines:
		{
			continue;
		}

		stringstream s(line);									// read an entire row and store it in a string variable 'line' 
		vector<string> row;
		while (getline(s, word, ','))							//  s used for breaking words 
		{

			row.push_back(word);								// add all the column data of a row to a vector 
		}
		mydata[i][0] = stoi(row[0]);							// stoi used to convert string to integer 
		mydata[i][1] = stoi(row[1]);
		mydata[i][2] = stoi(row[2]); 
		mydata[i][3] = stoi(row[3]);
		i++;
	}
	//total_edge = i;
	fin.close();
	return;
}
//*******************************read barn capacity file*****************

/*the read capacity method read the capacity of all farms and save
them in array to use as a weight
of farms in  SDTC equation*/
void read_capacity(string barnsize_file_name)
{
	ifstream fin;
	string strPathCSVFile = barnsize_file_name;     //"C:/Users/ansari/my_project/barn_size.txt";				// Open an existing file 
	fin.open(strPathCSVFile.c_str());
	if (!fin.is_open())
	{
		cout << "Path Wrong2!!!!" << endl;
		exit(EXIT_FAILURE);
	}
	string line;
	string word, temp;
	//int i = 0;
	//getline(fin, line);
	while (getline(fin, line))							       // Read the Data from the file 
	{
		if (line.empty())							           // skip empty lines:
		{
			continue;
		}

		stringstream s(line);									// read an entire row and store it in a string variable 'line' 
		vector<string> row;
		cout << "I'm there";
		while (getline(s, word, ','))							//  s used for breaking words 
		{

			row.push_back(word);								// add all the column data of a row to a vector 
		}
		for (int j = 0; j < node_num; j++)
			barn_size[j] = stoi(row[j]);							// stoi used to convert string to integer 

	}
	fin.close();
	return;

}

//*****************************************main ***************
int main()
{
	ofstream outdata1, outdata2;
	const char* task_id;
	double sumw[node_num], sumdelay[node_num];
	int temp;
	cout << "I'm here in main";

	//***************opening output file**************************
	string s1 = "sddc";
	string s2 = "sdtc";


	// for parallel execution of code on cluster find task id 
	try {
		task_id = getenv("SLURM_ARRAY_TASK_ID");
	}
	catch (const std::exception& e) { // reference to the base of a polymorphic object
		std::cout << e.what();
	}
	s1 = s1 + task_id + ".txt";
	s2 = s2 + task_id + ".txt";

	outdata1.open(s1);
	if (!outdata1)
	{									// file couldn't be opened
		cerr << "Error: file could not be opened" << endl;
		exit(1);
	}
	outdata2.open(s2);



	//**************************Reading from input file**********************************

	//cout << "barn size test is:" << barn_size[100] << barn_size[2] << barn_size[7000];
	string dataset_name, barnsize_file_name;
	//read 100 synthetic network datasets and barn capacities files and then compute measures for each dataset
	
	dataset_name = "/p/tmp/ansari/syndata22jul2021-v1.txt";    //the synthtic dataset includes animal movements;
	read_inputfile(dataset_name);
	barnsize_file_name = "/p/tmp/ansari/barn_size22jul2021-v1.txt"; //farm sizes
	read_capacity(barnsize_file_name);
	cout << "the file read successfully" << dataset_name << endl;
	cout << "the barn size read successfully" << barnsize_file_name << endl;



	//**********************initializing array for summation o js**************************
	for (int i = 0; i < node_num; i++)
	{
		sumdelay[i] = 0;
		sumw[i] = 0;
	}

	//****************************calling the main function newmeasure for all nodes as i0 and t0*******************************
	for (int i = 0; i < number_of_seeds; i++)
	{
		newmeasure(i, stoi(task_id));
		for (int j = 0; j < node_num; j++)                         // after running the function for one i0 and t0 do the following:
		{
			temp = delay[j];
			if (temp != 0)
				sumdelay[j] = sumdelay[j] + 1.0 / temp;          //do summation on 1/dj for all j 
			                                        
			sumw[j] = sumw[j] + wbig[j];                           // sum all wbig for all j 

		}
	}///**************************************writing results in file******************************************
	for (int j = 0; j < node_num; j++)                              //when finishing the execution for all i0s and t0s write outputs
	{
		outdata1 << sumdelay[j] / number_of_seeds << " ";
		outdata2 << sumw[j] / number_of_seeds << " ";

	}

	outdata1 << endl;
	outdata2 << endl;
	outdata1.close();
	outdata2.close();
	cout << "the program executed successfully";
	return 0;
}



