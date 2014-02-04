/* This is multi-threaded verison.
 * Our program has two phases. In the first phase, we divide the input file into
 * "runs" and sort those runs in memory. In the second phase, we merge those runs
 * into a single sorted output file */
#include "qsort.h"
void inMemorySort( PhaseOneInfo * const info, int bufIndex)
{
	std::ofstream tempFile;	
	std::string name;
	Buffer * inBuf;
	Buffer * outBuf;
	Buffer * lastBuf;
	Buffer ** inputBufferArray = info->inputBufferArray;	
	partialRecord parRec;
	recType rec;
	std::vector<recType> recordList;	
	int inBufferSize = info->inBufferSize;
	int index;
	ThreadVariables *threadVars = info->threadVars;
	outBuf = new Buffer;
	size_t outBufferSize = info->outBufferSize;
	outBuf->buffer = new char[outBufferSize];
	threadVars = info->threadVars;
	char * bufferEnd;
	char * lastBufferEnd;
	char * recordEnd;
	char * recordStart;
	bool partial = false;
	parRec.partial = false;
	char inputFileName[128]; 
	std::strncpy(inputFileName,info->inputFileName,127);
	/* assign a buffer for this thread */
	inBuf = inputBufferArray[bufIndex];
	/* each interation reads a chunk of input file, sorts and writes it to a temporary file */
	while(1)
	{
		{
			std::unique_lock<std::mutex> ul(threadVars->readyMutex);
			if (info->eof) break;
			/* threads compete for the next chunk of input file which is
			 * identified by index */
			index = ++threadVars->latestBuffer;
			info->bufIndex[index] = bufIndex;
			if (index > 0)
			{
				/* we go back to the last buffer to determine if we have a 
				 * partial record or not */
				lastBuf = inputBufferArray[info->bufIndex[index-1]];
				lastBufferEnd = lastBuf->buffer+lastBuf->bufferSize-1;
				/* we go backwards from the end of the last buffer 
				 * to find '\n' */
				for (char * i = lastBufferEnd; i>=lastBuf->buffer;i-- )
				{
					if (*i == '\n')
					{
						if (i == lastBufferEnd)
						{
							recordStart = NULL;
							break;
						}
						else
						{
							recordStart = i+1;
							
							break;
						}
					}
				}
				if (recordStart != NULL) /* we have a partial record */
				{
					parRec.size = lastBufferEnd-recordStart+1;
					std::memcpy(parRec.data,recordStart,parRec.size);
					partial = true;
				}
				else partial = false; /* we don't have a partial record */
			}
				
			info->inputFile.read(inBuf->buffer,inBufferSize);
			inBuf->bufferSize = info->inputFile.gcount();
			if ((info->inputFile.eof() == true) && (info->inputFile.gcount() == 0))
			{
				info->eof = true;
				info->numberOfRuns = index;
				break;
			}
		}

		if (partial == true)
		{
			recordEnd = (char*) std::memchr(inBuf->buffer,'\n',inBuf->bufferSize);
			std::memcpy(parRec.data + parRec.size,inBuf->buffer,recordEnd-inBuf->buffer+1);
			rec.size = parRec.size+recordEnd-inBuf->buffer+1;
			rec.data = parRec.data;
			recordList.push_back(rec);
			recordStart = recordEnd+1;	
		}
		else 
			recordStart = inBuf->buffer;
		bufferEnd = inBuf->buffer+inBuf->bufferSize-1;
		/*get all the records of the buffer*/
		while(1)
		{
			recordEnd = (char*)std::memchr(recordStart,'\n',bufferEnd-recordStart+1);
			if (recordEnd == NULL) /* we have a partial record */
			{
				break; /* we let the next iteration deal with this partial record*/
			}
			else  /* we don't have a partial record */
			{
				/* we have reached to the end of the buffer */
				if (recordEnd == bufferEnd)
				{
					/* put the record to the RecordList */
					rec.size = recordEnd-recordStart+1;
					rec.data = recordStart;
					recordList.push_back(rec);
					break;
				}
				else /* we have reached to the end of the buffer */
				{
					/* put the record to the RecordList */
					rec.size = recordEnd-recordStart+1;
					rec.data = recordStart;
					recordList.push_back(rec);
					recordStart = recordEnd+1;
				}
			}
		}

		/*sort the records*/
		std::sort(recordList.begin(),recordList.end());
		name = "temp" + std::to_string(index);
		tempFile.open(name,std::ios::binary);
		if (!tempFile.is_open())
		{
			std::cout<<"error opening temporary file"<<std::endl;
			exit(-1);
		}
		/*write records to buffer*/
		outBuf->bufferLength = 0;
		for (int i = 0; i < recordList.size(); i++)
		{
			std::memcpy(outBuf->buffer+outBuf->bufferLength,recordList[i].data,recordList[i].size);
			outBuf->bufferLength += recordList[i].size;
		/*write buffer to file*/
		if (outBuf->bufferLength > (outBufferSize - MAX_LENGTH))
		{
			tempFile.write(outBuf->buffer,outBuf->bufferLength);
			outBuf->bufferLength = 0;
		}
		}
		/* write the records of the last buffer to the output file */
		if (outBuf->bufferLength > 0)
		{
			tempFile.write(outBuf->buffer,outBuf->bufferLength);
		}
		recordList.clear();
		tempFile.close();
	}
	delete[] outBuf->buffer;
}
int phase1(char * inputFileName)
{
	int numberOfBuffers = 2;
	size_t inBufferSize = 100000020; /* the size of the input buffer which determines 
                                          * the size of each run */
	size_t outBufferSize = 10000000; /* the size of the output buffer */
	PhaseOneInfo * info;
	/* initialize info structure*/
	info = new PhaseOneInfo;
	info->inputBufferArray = new Buffer*[numberOfBuffers];
	info->threadVars = new ThreadVariables;		
	info->threadVars->latestBuffer = -1;
	info->inBufferSize = inBufferSize;
	info->outBufferSize = outBufferSize;
	info->numberOfBuffers = numberOfBuffers;
	info->bufIndex = new int[128];
	info->eof = false;
	std::strncpy(info->inputFileName,inputFileName,127);
	/* initialize input buffers */
	for (int i = 0; i < numberOfBuffers; i++)
	{
		info->inputBufferArray[i] = new Buffer;
		info->inputBufferArray[i]->buffer = new char[inBufferSize];
		info->inputBufferArray[i]->bufferSize = inBufferSize;
	}
	info->inputFile.open(inputFileName,std::ios::binary);
	if (!info->inputFile.is_open())
	{
		std::cout<<"error opening input file"<<std::endl;
		exit(-1);
	}
	/* spawn a thread to sort the records*/ 
	std::thread t1(inMemorySort,info,0);
	/* sort the records in main thread  */
	inMemorySort(info,1);
	t1.join();
	info->inputFile.close();
	for (int i = 0; i < numberOfBuffers; i++)
	{
		delete[] info->inputBufferArray[i]->buffer;
	}
	return info->numberOfRuns;
}

void phase2(char* outputFileName, int numberOfFiles)
{	
	
	int bufferSize = 15000000; /* size of the input buffers*/
	char * recordStart; /* start position of the record */
	char * recordEnd;   /* end position of the record */
	recType rec;
	int index;          /* identify the run we are dealing with */
	std::string tempName = "temp";
	PhaseTwoInfo * info;
	/* initialize struct info */
	info = new PhaseTwoInfo;
	info->numberOfFiles = numberOfFiles;
	info->inputFile = new std::ifstream *[numberOfFiles];
	info->inputBufferArray = new Buffer*[numberOfFiles];
	info->parRec = new partialRecord*[numberOfFiles];
	info->currentRecord = new int[numberOfFiles];
	info->endOfBuffer = new bool[numberOfFiles];
	info->eof = new bool[numberOfFiles];
	info->outputFile = new std::ofstream;
	info->outputFile->open(outputFileName,std::ios::binary);
	if (!info->outputFile->is_open())
	{
		std::cout<<"error opening output file"<<std::endl;
		exit(-1);
	}
	info->outputBuffer = new Buffer;
	info->outputBuffer->buffer = new char[bufferSize+MAX_LENGTH];
	info->outputBuffer->bufferSize = bufferSize+MAX_LENGTH;
	info->outputBuffer->bufferLength = 0;
	info->bufferSize = bufferSize;
	

	for (int i = 0; i < numberOfFiles; i++)
	{
		//info->eof[i] = new bool;
		//info->endOfBuffer[i] = new bool;
		//info->currentRecord[i] = new int;
		info->parRec[i] = new partialRecord;
		info->parRec[i]->partial = false;
		info->inputBufferArray[i] = new Buffer;
		info->inputBufferArray[i]->buffer = new char[bufferSize];
		info->inputBufferArray[i]->bufferSize = bufferSize;
		info->inputFile[i] = new std::ifstream;
		info->inputFile[i]->open(tempName+std::to_string(i),std::ios::binary);
		info->inputFile[i]->read(info->inputBufferArray[i]->buffer, bufferSize);
		info->eof[i] = false;
		info->endOfBuffer[i] = false;
		/* initialize the RecordList*/
		recordStart = info->inputBufferArray[i]->buffer;
		recordEnd = (char*) std::memchr(recordStart,'\n', info->inputBufferArray[i]->buffer+10000-recordStart);
		rec.size = recordEnd-recordStart+1;
		rec.data = recordStart;
		info->currentRecord[i] = rec.size;
		info->recordList.push_back(rec);
	}
	/* our main loop */
	while ((index = getLeast(info)) != -1)
	{
		//std::cout<<"index "<<index<<std::endl;
		putOut(info, index);
		getNext(info,index);
	}
	/* write the records of the last buffer  to the output file */
	if (info->outputBuffer->bufferLength != 0)
	{
		info->outputFile->write(info->outputBuffer->buffer,info->outputBuffer->bufferLength);
	}
}
inline void getNext(PhaseTwoInfo * info,int index  )
{
	char * recordStart;/* start position of the record */
	char * recordEnd;  /* end position of the record */
	char * bufferEnd;  /* end position of the buffer */
	Buffer *inputBuffer;
	inputBuffer = info->inputBufferArray[index];
	partialRecord *parRec = info->parRec[index];
	std::ifstream *inputFile = info->inputFile[index];

	if (info->eof[index] == true) return; /* if this run is out of records we do nothing*/
	if (info->endOfBuffer[index] == true)/*we have reached to the end of the buffer 
                                              * so now we refill it */
	{
		inputFile->read(inputBuffer->buffer,inputBuffer->bufferSize);
		/* if this run is out of records we indicate it and exit */
		if (inputFile->eof() == true && inputFile->gcount() == 0) 
		{
			info->eof[index] = true;
			return;
		}

		inputBuffer->bufferSize = inputFile->gcount();
		bufferEnd = inputBuffer->buffer+inputBuffer->bufferSize-1;
		if (parRec->partial == true ) /* we have a partial record */
		{
			/* copy the rest of the record to parRec */
			recordEnd = (char *)std::memchr(inputBuffer->buffer,'\n',inputBuffer->bufferSize);
			std::memcpy(parRec->data + parRec->size,inputBuffer->buffer,
				recordEnd-inputBuffer->buffer+1);
			info->recordList[index].size = parRec->size+recordEnd-inputBuffer->buffer+1;
			info->recordList[index].data = parRec->data;
			if (recordEnd == bufferEnd  ) /* we have reached to the end of the buffer */
			{
				info->endOfBuffer[index] == true;
				parRec->partial = false;
			}
			else /* we have not reached to the end of the buffer */
			{
				info->currentRecord[index] = recordEnd + 1-inputBuffer->buffer;
				parRec->partial = false;
				info->endOfBuffer[index] = false;
			}
		}
		else /*we get a record at the beginning of the buffer*/
		{
			recordStart = inputBuffer->buffer;
			recordEnd = (char*) std::memchr(recordStart,'\n', bufferEnd-recordStart+1);
			info->recordList[index].size = recordEnd-recordStart+1;
			info->recordList[index].data = recordStart;
			info->currentRecord[index] = recordEnd + 1-inputBuffer->buffer;
			info->endOfBuffer[index] = false;
		}

	}
	else //we are in the middle of the buffer
	{
		bufferEnd = inputBuffer->buffer+inputBuffer->bufferSize-1;
		recordStart = inputBuffer->buffer+info->currentRecord[index];
		recordEnd = (char*) std::memchr(recordStart,'\n', bufferEnd-recordStart+1);
		if (recordEnd == NULL) /* we have a partial record at the end of the buffer */
		{
			/* copy part of the record to the partial record */
			parRec->size = bufferEnd-recordStart+1;
			std::memcpy(parRec->data,recordStart,parRec->size);
			parRec->partial = true;
			info->endOfBuffer[index] = true;
		}
		else /* we don't have a partial record*/
		{
			if (recordEnd == bufferEnd) /* we have reached to the end of the buffer */
			{
				/* put the record to the RecordList */
				info->recordList[index].size = recordEnd-recordStart+1;
				info->recordList[index].data = recordStart;	
				info->endOfBuffer[index] = true;
			}
			else /* we have not reached to the end of the buffer */
			{
				/* put the record to the RecordList */
				info->recordList[index].size = recordEnd-recordStart+1;
				info->recordList[index].data = recordStart;
				info->currentRecord[index] = recordEnd + 1-inputBuffer->buffer;
			}
		}
	}
}
inline void putOut(PhaseTwoInfo *info, int index)
{
	Buffer *outputBuffer = info->outputBuffer;
	partialRecord *parRec = info->parRec[index];
	/* we only deal with runs that still have records
	 * and full records (not partial ones)*/
	if (info->eof[index] == false && parRec->partial == false)
	{
		std::memcpy(outputBuffer->buffer+outputBuffer->bufferLength,
			    info->recordList[index].data,info->recordList[index].size);
		outputBuffer->bufferLength += info->recordList[index].size;

	}
	/* write the records of the last buffer to the output file */
	if (outputBuffer->bufferLength > info->bufferSize)
	{
		info->outputFile->write(outputBuffer->buffer,outputBuffer->bufferLength);
		outputBuffer->bufferLength = 0;
	}
}
inline int getLeast(PhaseTwoInfo *info)
{
	int index = -1; /* identify a run */
	char min[MAX_LENGTH];
	int numberOfFiles = info->numberOfFiles;
	/* find the first record in the RecordList, 
	 * choose it as the record with the least value*/
	for (int i = 0; i<numberOfFiles;i++)
	{
		if (info->eof[i] == false)
		{
			std::memcpy(min, info->recordList[i].data,info->recordList[i].size);
			index = i;
			break;
		}
	}
	/* determine the record with the least value int the RecordList*/
	for (int i = 0; i<numberOfFiles;i++)
	{
		if (info->eof[i] == false)
		{
			if((std::memcmp(info->recordList[i].data,min,info->recordList[i].size) < 0))
			{
				std::memcpy(min, info->recordList[i].data,info->recordList[i].size);
				index = i;
			}
		}
	}
	/* return the index of the run */
	return index;
}
