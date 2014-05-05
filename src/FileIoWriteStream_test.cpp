#include "FileIoStreams.h"

#include <cassert>
#include <cstdio>
#include <cstring>

void FileIoWriteStream_test()
{
    printf( "> FileIoWriteStream_test()\n" );

    printf( "opening " );

    const char *testFileName = "..\\..\\..\\FileIoWriteStream_test_output.txt";
    // in msvc this is a path relative to the project directory:
    SharedBuffer *path = SharedBufferAllocator::alloc(testFileName);
    READSTREAM *fp = FileIoWriteStream_open(path, FileIoRequest::READ_WRITE_OVERWRITE_OPEN_MODE);
    path->release();
    assert( fp != 0 );

    while (FileIoWriteStream_pollState(fp) == STREAM_STATE_OPENING) {
        printf(".");
        Sleep(10);
    }

    printf( "\ndone.\n" );

    assert( FileIoWriteStream_pollState(fp) == STREAM_STATE_OPEN_IDLE );

    printf( "seeking " );

    FileIoWriteStream_seek(fp, 0);

    while (FileIoWriteStream_pollState(fp) == STREAM_STATE_OPEN_BUFFERING) {
        printf(".");
        Sleep(10);
    }
    
    assert( FileIoWriteStream_pollState(fp) == STREAM_STATE_OPEN_STREAMING );

    printf( "\ndone.\n" );

    printf( "writing:\n" );

    const int lineCount = 100000;

    // write 1000 integers in text from 0 to lineCount, one per line
    for (int i=0; i < lineCount; ++i) {

        char s[128];
        std::sprintf(s, "%d\n", i);
        size_t bytesToWrite = std::strlen(s);

        // write bytes out one at a time because the current implementation of write requires items to be block-aligned
        for (int j=0; j <bytesToWrite; ++j) {
            FileIoWriteStream_write( &s[j], 1, 1, fp );
        }
    }

    printf( "\nclosing.\n" );

    FileIoWriteStream_close(fp);

    // verify the test file contents
    {
        printf( "\nverifying...\n" );

        Sleep(1000); // pray that file has been closed by now

        FILE *fp = std::fopen(testFileName, "rt");
        assert( fp != 0 );

        // read 1000 lines, verify that each contains the integers in [0,lineCount)
        for (int i=0; i < lineCount; ++i) {
            char s[1024];
            std::fgets(s, 128, fp);

            int value = -1;
            std::sscanf(s, "%d", &value);
            assert( value == i );
        }

        std::fclose(fp);
    }

    printf( "\ndone.\n" );
    
    printf( "< FileIoWriteStream_test()\n" );
}
