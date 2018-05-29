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

int main(int argc, char **argv) 
{

	remove(argv[2]);
	char * inputFile = argv[1];
	char * outputFile = argv[2];
	char * mapper = argv[3];
	char * reducer = argv[4];
	int mapcount = atoi(argv[5]);
	int reducercount = atoi(argv[argc-1]);

	//char *inicheck[reducercount];


	pid_t childrenmap[mapcount];
	pid_t childrensplit[mapcount];
	pid_t reducerfork[reducercount];

	int mappipe[mapcount][2];
	int shufflerpipe[2];

	pipe2(shufflerpipe, O_CLOEXEC);

	for(int i=0;i<mapcount;i++)
	{
	//childrenmap[i]=-2;
		pipe2(mappipe[i], O_CLOEXEC);
	}
	pid_t check;
	//splitter
	//printf("started splitter\n");
	for(int i=0;i<mapcount;i++)
	{
		check = fork();
		if(check==0)
		{
		  //close(mappipe[i][0]);
		  dup2(mappipe[i][1], STDOUT_FILENO);
		  char strI[30];
		  sprintf(strI, "%d", i);
		  for(int j=0;j<mapcount;j++)
		  {
			close(mappipe[j][0]);
		   	close(mappipe[j][1]);
		  }
		  close(shufflerpipe[0]);
		  close(shufflerpipe[1]);
		  execl("./splitter","./splitter", inputFile, argv[5], strI, (char*)NULL);
		  exit(2);
		}
		else
		{
		  childrensplit[i] = check;
		}
	}
	//printf("ending splitter\n");


	//mapper
	//printf("started mapper\n");

	for(int i=0;i<mapcount;i++)
	{
		check = fork();
		if(check==0)
		{
			dup2(shufflerpipe[1], STDOUT_FILENO);
			dup2(mappipe[i][0], STDIN_FILENO);
			for(int j=0;j<mapcount;j++)
			{
				close(mappipe[j][0]);
			   	close(mappipe[j][1]);
			}
			close(shufflerpipe[0]);
			close(shufflerpipe[1]);
			execl(mapper,mapper, (char*)NULL);
			exit(2);
		}
		else
		{
			childrenmap[i]=check;
		}
	}
	//printf("ending mapper\n");



	//creating files and starting shuffler
	//printf("started shuffler\n");

	char *filestarting = strdup("./fifo_");
	char *shufflerfiles[reducercount+2];

	shufflerfiles[0] = strdup("./shuffler");
	for(int i=1;i<reducercount+1;i++)
	{
		shufflerfiles[i] = malloc(strlen(filestarting) + 4);
		strcpy(shufflerfiles[i], filestarting);
		char strI[30];
		sprintf(strI, "%d", i-1);
		strcat(shufflerfiles[i], strI);
		remove(shufflerfiles[i]);
		int filecheck = mkfifo(shufflerfiles[i], S_IRWXU);
		if(filecheck==-1)
		{
			printf("mkfifo failed to create a file\n");
			exit(1);
		}
	}
	shufflerfiles[reducercount+1] = NULL;
	free(filestarting);

	//printf("printing file names\n");
	// for(int i=0;i<reducercount+1;i++)
	// {
	// 	printf("%s\n", shufflerfiles[i]);
	// }
	pid_t shufflerfork;
	shufflerfork = fork();
	if(shufflerfork==0)
	{	
		//printf("entering shufflerchild\n");
		dup2(shufflerpipe[0], STDIN_FILENO);
		for(int j=0;j<mapcount;j++)
	  	{
			close(mappipe[j][0]);
		   	close(mappipe[j][1]);
	  	}
		close(shufflerpipe[0]);
	  	close(shufflerpipe[1]);
		execvp("./shuffler",shufflerfiles);
		exit(2);
	}
	//printf("ending shuffler\n");


	//reducer
	//printf("started reducer\n");

	FILE * opFile = fopen(outputFile, "a");
	int fileDW = fileno(opFile);

	for(int i=0;i<reducercount;i++)
	{
		check = fork();
		if(check==0)
		{
			//printf("starting reducer %d\n", i);
			FILE * sfptr = fopen(shufflerfiles[i+1], "r");
		  	int fds = fileno(sfptr);
		  	dup2(fds,STDIN_FILENO);
		  	dup2(fileDW, STDOUT_FILENO);
		  	for(int j=0;j<mapcount;j++)
			{
				close(mappipe[j][0]);
				close(mappipe[j][1]);
			}
			close(shufflerpipe[0]);
			close(shufflerpipe[1]);
			close(fds);
			execl(reducer,reducer, (char*)NULL);
			exit(2);
		}
		else
		{
			//printf("Updating reducerfork %d\n", i);
			reducerfork[i] = check;
			//printf("%d\n", reducerfork[i]);
		}
	}
	fclose(opFile);
	//printf("ending reducer\n");

	//printf("started closing\n");
	close(shufflerpipe[0]);
	close(shufflerpipe[1]);
	for(int i = 0; i < mapcount; i++)
	{
		close(mappipe[i][0]);
		close(mappipe[i][1]);
	}
	//printf("ending closing\n");

	//printf("Reached here 1\n");

	//printf("started waiting\n");

	int status1 = 0;
	waitpid(shufflerfork, &status1, 0);
	//printf("ending status1\n");


	int status2[mapcount];
	for(int i=0;i<mapcount;i++)
	{
		status2[i]=0;
		waitpid(childrenmap[i], &status2[i], 0);
	}

	//printf("ending status2\n");

	//printf("Reached here 2\n");
	int status3[mapcount];
	for(int i=0;i<mapcount;i++)
	{
		status3[i] = 0;
		waitpid(childrensplit[i], &status3[i], 0);
	}
	//printf("ending status3\n");

	int status4[reducercount];
	for(int i=0;i<reducercount;i++)
	{
		status4[i] = 0;
		//printf("Waiting for reducer %d\n", i);
		waitpid(reducerfork[i], &status4[i], 0);

	}
		  //printf("ending status4\n");

	//printf("ending waiting\n");


	int mapExit;
	int redExit;

	//printf("Reached here 3\n");

	for(int i=0;i<mapcount;i++)
	{
		if((mapExit=WEXITSTATUS(status2[i]))!=0)
			printf("%s %d exited with status %d\n", mapper, i, mapExit);
	}

	for(int i=0;i<reducercount;i++)
	{
		if((redExit=WEXITSTATUS(status4[i]))!=0)
			printf("%s %d exited with status %d\n", reducer, i, redExit);
	}

	FILE * countOP = fopen(outputFile, "r");
	int count = 0;
	char * line = NULL;
	size_t temp;

	// for(int i=0;i<reducercount;i++)
	// {
	// 	unlink(shufflerfiles[i+1]);
	// }

	for(int i=0;i<reducercount+2;i++)
	{
		free(shufflerfiles[i]);
	}
	//unlink(argv[2]);

	while(getline(&line, &temp, countOP) > 0)
	{
		count++;
	}
	free(line);

	printf("output pairs in %s: %d\n",outputFile, count);
	fclose(countOP);
	return 0;
}