#include "FileIoStreams.h"

#include <cassert>
#include <cstdio>

#ifndef WIN32
#include <unistd.h> // for usleep
#define Sleep(milliseconds) usleep((milliseconds)*1000)
#endif


void FileIoReadStream_test()
{
    printf( "> FileIoReadStream_test()\n" );

    printf( "opening " );

    // in msvc this is a path relative to the project directory:
#ifdef WIN32
    const char *pathString = "..\\..\\..\\src\\FileIoReadStream_test.cpp";
#else
    const char *pathString = "../../../src/FileIoReadStream_test.cpp";
#endif    
    SharedBuffer *path = SharedBufferAllocator::alloc(pathString); // print out the source code of this file

    READSTREAM *fp = FileIoReadStream_open(path, FileIoRequest::READ_ONLY_OPEN_MODE);
    path->release();
    assert( fp != 0 );

    while (FileIoReadStream_pollState(fp) == STREAM_STATE_OPENING) {
        printf(".");
        Sleep(10);
    }

    printf( "\ndone.\n" );

    assert( FileIoReadStream_pollState(fp) == STREAM_STATE_OPEN_IDLE );

    printf( "seeking " );

    FileIoReadStream_seek(fp, 0);

    while (FileIoReadStream_pollState(fp) == STREAM_STATE_OPEN_BUFFERING) {
        printf(".");
        Sleep(10);
    }
    
    assert( FileIoReadStream_pollState(fp) == STREAM_STATE_OPEN_STREAMING );

    printf( "\ndone.\n" );

    printf( "reading:\n" );

    while (FileIoReadStream_pollState(fp) == STREAM_STATE_OPEN_STREAMING || FileIoReadStream_pollState(fp) == STREAM_STATE_OPEN_BUFFERING) {

        /*
        // make sure we're always streaming:
        while (FileIoReadStream_pollState(fp) == STREAM_STATE_OPEN_BUFFERING) {
            printf(".");
            Sleep(10);
        }
        */

        char c[512];
        size_t bytesToRead = rand() & 0xFF;
        size_t bytesRead = FileIoReadStream_read( c, 1, bytesToRead, fp );
        if (bytesRead>0) {
            fwrite(c, 1, bytesRead, stdout);
        }
    }

    assert( FileIoReadStream_pollState(fp) == STREAM_STATE_OPEN_EOF );

    printf( "\nclosing.\n" );

    FileIoReadStream_close(fp);

    printf( "< FileIoReadStream_test()\n" );
}
