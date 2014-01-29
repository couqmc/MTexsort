/* Our program has two phases. In the first phase, we divide the input file into
 * "runs" and sort those runs in memory. In the second phase, we merge those runs
 * into a single sorted output file */
#include<future>
#include<thread>
#include<chrono>
#include<condition_variable>
#include<mutex>
#include<stdio.h>
#include<iostream>
#include<string>
#include<cstring>
#include<fstream>
#include<time.h>
#include<algorithm>
#include <chrono>
//#include <chrono_io>
#define MAX_LENGTH 1024
typedef std::chrono::high_resolution_clock Clock;
/* struct for variables to be communicated between threads*/
struct ThreadVariables {
	int latestBuffer;
	std::mutex readyMutex;
};
/* struct for a record */
struct recType 
{
	char *data; /*point to memory allocated for the record*/
	int size;   /* size of the record */
	bool operator<(const recType &a) const
	{
		if((data == NULL) || (a.data == NULL)) return true;
		return (std::memcmp(data, a.data,MAX_LENGTH) < 0);
	}
} ;
/* struct for a partial record, i.e, a record that spans two buffers */
struct partialRecord 
{
	char data[1024]; /* store the content of the partial record */
	int size;        /* size of the record*/
	bool partial;    /* indicate if we hava a partial record between 
	                  * two consecutive buffers or not*/
};
/* struct for a buffer */
struct Buffer 
{
	char * buffer;    /* point to memory allocated for the buffer */
	int bufferSize;   /* size of the buffer */
	int bufferLength; /* current length of the buffer */
};
struct PhaseOneInfo
{
	Buffer **inputBufferArray; /* point to array of buffers, each buffer 
                                    * is used for a corresponding thread */
	size_t inBufferSize;       /* size of input buffer */
	size_t outBufferSize;      /* size of output buffer */
	int numberOfBuffers;       /* number of input buffers */
	char inputFileName[128];   /* input file name */
	int *bufIndex;             /* identify a buffer */ 
	ThreadVariables * threadVars; /* store vairables to be communicated between threads */
	std::ifstream inputFile;   /* input file stream */
	bool eof;                  /* output file stream */
	int numberOfRuns;          /* number of runs produced by phase 1 */ 
};
struct PhaseTwoInfo
{
	Buffer** inputBufferArray; /* point to array of buffers, each buffer 
                                    * corresponding to a run produced by phase 1 */
	size_t bufferSize;         /* common size for all buffers*/
	std::ifstream **inputFile; /* input file stream */
	std::ofstream *outputFile; /* output file stream */
	partialRecord** parRec;    /* partial record for each run */
	int *currentRecord;        /* current position of a record in the input buffer */
	std::vector<recType> recordList;/* each record of the RecordList corresponding
	                                 * to a run */
	Buffer *outputBuffer;      /* output buffer */
	bool *endOfBuffer;         /* indicate if we are at the end of the buffer or not */
	bool *eof;                 /* indicate if we are out of records of the run or not */ 
	int numberOfFiles;
};

/* produce runs that are sorted in-memory, each run has size around the size
 * of the input buffer */
int  phase1(char* inputFileName);
/* we do the in-memory sorting here */

void inMemorySort( PhaseOneInfo * const info, int bufIndex);
/* multi-way merge runs of phase 1.
 * We pick one record of each run and put it into the RecordList.
 * Next we choose the record with least value and put it to the output.
 * Then we pick the next record from the run that just had the record  put out
 * and put it to the RecordList.
 * We repeat these steps until there are no input records.
 */
void phase2(char* outputFileName, int numberOfFiles);

/* get the next record from the run that just had a record  put out*/
void getNext(PhaseTwoInfo * info, int index);

/* put the record with the least value from the RecordList 
 * to the output buffer and then the output file
 */
void putOut(PhaseTwoInfo * info, int index); 

/* get the record with the least value in the RecordList.
 * Return the index corresponding to the run that the record is from.
 */
int getLeast(PhaseTwoInfo * info);
