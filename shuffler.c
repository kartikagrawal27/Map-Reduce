/**
 * MapReduce
 * CS 241 - Spring 2016
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include "common.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>

#include "common.h"

void usage() {
  fprintf(stderr, "shuffler destination1 destination2 ...\n");
  fprintf(stderr, "where destination1..n are files on the filesystem to which "
                  "the shuffler will write its output\n");
}

int main(int argc, char *argv[]) {
  // read from stdin
  // hash the key for the input line
  // send them to the correct output file (output files are given as command
  // line arguments
  if (argc < 2) 
  {
    usage();
    exit(1);
  }

  //printf("argc's value is %d\n", argc);
  //printf("Enter shuffler\n");
  int N = argc-1;
  char * line = NULL;
  size_t temp;
  char *key;
  char *values;

  FILE *fptr[N];

  for(int i=0;i<N;i++)
  {
    fptr[i] = fopen(argv[i+1], "w+");
  }

  while(getline(&line, &temp, stdin) != -1)
  {
    //printf("in shuffler loop\n");
    int retval = split_key_value(line,  &(key),  &(values));

    if(retval)
    {
      unsigned int mykey = hashKey(key);
      unsigned filenum = mykey % N;
      fprintf(fptr[filenum], "%s: %s\n", key, values);
    }
    else
      continue;
    //printf("shuffler loop end\n");

    //printf("in loop\n");
  }
  //printf("in shuffler.c\n");
  free(line);

  for(int i=0;i<N;i++)
  {
    fclose(fptr[i]);
  }
  //printf("sup man\n");
  return 0;
}
