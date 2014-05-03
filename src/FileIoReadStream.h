/* 
    Real Time File Streaming copyright (c) 2014 Ross Bencina

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/
#ifndef INCLUDED_FILEIOREADSTREAM_H
#define INCLUDED_FILEIOREADSTREAM_H

#include "FileIoRequest.h"
#include "SharedBuffer.h"

// NOTE: all functions declared here are real-time safe

typedef void READSTREAM;

READSTREAM *FileIoReadStream_open( SharedBuffer *path, FileIoRequest::OpenMode openMode ); 

void FileIoReadStream_close( READSTREAM *fp );

int FileIoReadStream_seek( READSTREAM *fp, size_t pos ); // returns non-zero if there's a problem

size_t FileIoReadStream_read( void *dest, size_t itemSize, size_t itemCount, READSTREAM *fp );

enum FileIoReadStreamState {
    // stream states

    STREAM_STATE_OPENING = FileIoRequest::CLIENT_USE_BASE_,
    STREAM_STATE_OPEN_IDLE,
    STREAM_STATE_OPEN_EOF,
    STREAM_STATE_OPEN_BUFFERING,
    STREAM_STATE_OPEN_STREAMING,
    STREAM_STATE_ERROR,
};

FileIoReadStreamState FileIoReadStream_pollState( READSTREAM *fp );

int FileIoReadStream_getError( READSTREAM *fp ); // returns the error code. only returns non-zero if pollState returns READSTREAM_STATE_ERROR

void FileIoReadStream_test();


#endif /* INCLUDED_FILEIOREADSTREAM_H */
