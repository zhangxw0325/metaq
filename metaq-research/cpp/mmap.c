/**
 * $Id: mmap.cpp 1595 2012-07-28 10:33:18Z shijia.wxr $
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>

#define MMAP_FILE_SIZE (1024 * 1024 * 1024)

int main(int argc, char** argv)
{
	int i = 0;

	for(i = 1; ; i++) {
		char filePath[64] = {0};
		sprintf(filePath, "%d", i);
		int fd = open(filePath, O_RDWR | O_CREAT);
		if(fd < 0) {
			printf("open failed\n");
			break;
		}
		ftruncate(fd, MMAP_FILE_SIZE);

		void* p = mmap(0, MMAP_FILE_SIZE, PROT_WRITE, MAP_SHARED,fd, 0);
		if(p != 0) {
			memset(p, 0, MMAP_FILE_SIZE);
			printf("mmap OK, %d\n", i);
		} else {
			printf("mmap Failed, exit.\n");
			break;
		}
	}

	while (1) {
		printf("sleeping\n");
		sleep(1);
	}

	return 0;
}