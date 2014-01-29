CXXFLAGS  = -std=c++0x -g -Wall
CC=g++
OBJECTS=main.o qsort.o
all : exsort
clean : 
	rm *.o exsort
exsort:$(OBJECTS)
	$(CC) $(CXXFLAGS) -o exsort -pthread $(OBJECTS)
main.o: main.cpp qsort.h
	$(CC) -c $(CXXFLAGS) -o main.o main.cpp
qsort.o: qsort.cpp qsort.h
	$(CC) -c $(CXXFLAGS) -o qsort.o qsort.cpp
