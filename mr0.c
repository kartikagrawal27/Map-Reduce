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

void usage() {
  printf("./mr0 input_file output_file mapper_exec reducer_exec\n");
}

extern char **environ;

int main(int argc, char **argv) {

  if(argc != 5)
  {
    usage();
    return -1;
  }
  char * inputFile = argv[1];
  char * outputFile = argv[2];
  char * mapper = argv[3];
  char * reducer = argv[4];

  int pipe[2];
  pipe2(pipe, O_CLOEXEC);

  int fileDR = open(inputFile, O_RDONLY);
  pid_t childM = fork();

  if(childM == 0)
  {
    close(pipe[0]);
    dup2(fileDR, STDIN_FILENO);
    dup2(pipe[1], STDOUT_FILENO);
    close(pipe[1]);
    execl(mapper,mapper, (char*)NULL);
    exit(2);
  }
  FILE * opFile = fopen(outputFile, "w+");
  int fileDW = fileno(opFile);
  pid_t childR = fork();

  if(childR == 0)
  {
    dup2(pipe[0], STDIN_FILENO);
    dup2(fileDW, STDOUT_FILENO);
    close(pipe[1]);
    execl(reducer,reducer, (char*)NULL);
    exit(2);
  }
  close(pipe[0]);
  close(pipe[1]);

  int status1 = 0;
  waitpid(childR, &status1, 0);
  int status2 = 0;
  waitpid(childM, &status2, 0);

  int mapExit = WEXITSTATUS(status2);
  int redExit = WEXITSTATUS(status1);

  if(mapExit!=0)
    printf("%s exited with status %d\n", mapper, mapExit);
  if(redExit!=0)
  printf("%s exited with status %d\n", reducer, redExit);

  FILE * countOP = fopen(outputFile, "r");
  int count = 0;
  char * line = NULL;
  size_t temp;

  while(getline(&line, &temp, countOP) > 0)
  {
    count++;
  }

  printf("output pairs in test.out: %d\n", count);
  fclose(countOP);
  return 0;
}