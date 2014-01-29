/* This is multi-threaded verison
 * Our program has two phases. In the first phase, we divide the input file into
 * "runs" and sort those runs in memory. In the second phase, we merge those runs
 * into a single sorted output file */
#include "qsort.h"
int main (int argc, char * argv[])
{
	int numberOfRuns;
	if (argc != 3)
	{
		std::cout<< "Usage: exsort <inputFileName> <outputFileName>";
		exit(-1);
	}
	
	auto t1 = Clock::now();
	numberOfRuns = phase1(argv[1]); /* phase 1 */
	phase2(argv[2],numberOfRuns);   /* phase 2 */
	auto t2 = Clock::now();
	std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
	std::cout <<  "Time to sort is " << time_span.count() << '\n';
	
	return 0;
}
