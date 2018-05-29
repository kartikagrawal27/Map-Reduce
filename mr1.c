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

void usage() 
{
  printf("./mr1 input_file output_file mapper_exec reducer_exec num_mappers\n");
}

int main(int argc, char **argv) {

  if(argc != 6)
  {
    usage();
    return -1;
  }
  char * inputFile = argv[1];
  char * outputFile = argv[2];
  char * mapper = argv[3];
  char * reducer = argv[4];
  int mapcount = atoi(argv[5]);

  pid_t childrenmap[mapcount];
  pid_t childrensplit[mapcount];

  int mappipe[mapcount][2];
  int redpipe[2];
  pipe2(redpipe, O_CLOEXEC);

  for(int i=0;i<mapcount;i++)
  {
    childrenmap[i]=-2;
    pipe2(mappipe[i], O_CLOEXEC);
  }

  //int fileDR = open(inputFile, O_RDONLY);

  pid_t check;

  for(int i=0;i<mapcount;i++)
  {
      check = fork();

      if(check==0)
      {
        //close(mappipe[i][0]);
        dup2(redpipe[1], STDOUT_FILENO);
        dup2(mappipe[i][0], STDIN_FILENO);
        close(mappipe[i][1]);
        close(redpipe[0]);
        execl(mapper,mapper, (char*)NULL);
        exit(2);
      }
      else
      {
        childrenmap[i]=check;
      }
  }

  FILE * opFile = fopen(outputFile, "w+");
  int fileDW = fileno(opFile);
  pid_t childR = fork();

  if(childR == 0)
  {
    dup2(redpipe[0], STDIN_FILENO);
    dup2(fileDW, STDOUT_FILENO);
    close(redpipe[1]);
    execl(reducer,reducer, (char*)NULL);
    exit(2);
  }
  // close(pipe[0]);
  // close(pipe[1]);

  // char* splitterargs[mapcount][5];

  // for(int i=0;i<mapcount;i++)
  // {
  //   splitterargs[i][0] = "./splitter";
  //   splitterargs[i][1] = inputFile;
  //   splitterargs[i][2] = argv[5];
  //   splitterargs[i][3] = (char *)&i;
  //   splitterargs[i][4] = NULL;
  // }

  for(int i=0;i<mapcount;i++)
  {
    check = fork();

    if(check==0)
    {
      close(mappipe[i][0]);
      //dup2(fileDR, STDIN_FILENO);
      dup2(mappipe[i][1], STDOUT_FILENO);
      //close(mappipe[i][1]);
      char strI[30];
      sprintf(strI, "%d", i);
      execl("./splitter","./splitter", inputFile, argv[5], strI, (char*)NULL);
      exit(2);
    }
    else
    {
      childrensplit[i] = check;
    }
  }

  close(redpipe[1]);
  close(redpipe[0]);
  for(int i = 0; i < mapcount; i++)
  {
    close(mappipe[i][0]);
    close(mappipe[i][1]);
  }

  int status1 = 0;
  waitpid(childR, &status1, 0);

  int status2[mapcount];
  for(int i=0;i<mapcount;i++)
  {
    status2[i]=0;
  }
  for(int i=0;i<mapcount;i++)
  {
     waitpid(childrenmap[i], &status2[i], 0);
  }
  int status3[mapcount];
  for(int i=0;i<mapcount;i++)
  {
    status3[i] = 0;
     waitpid(childrensplit[i], &status3[i], 0);
  }

  int mapExit;
  int redExit = WEXITSTATUS(status1);

  for(int i=0;i<mapcount;i++)
  {
    if((mapExit=WEXITSTATUS(status2[i]))!=0)
      printf("%s %d exited with status %d\n", mapper, i, mapExit);
  }

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

  printf("output pairs in %s: %d\n",outputFile, count);
  fclose(countOP);
  return 0;
}