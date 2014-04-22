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
#ifndef INCLUDED_FILEIOSERVER_H
#define INCLUDED_FILEIOSERVER_H

#define MAX_FILE_IO_REQUESTS    (1024)

struct FileIoRequest;

// public interface to the file I/O server:

// initialize and terminate the server
void startFileIoServer( size_t fileIoRequestCount=MAX_FILE_IO_REQUESTS);
void shutDownFileIoServer();

// allocate and release requests from the global request pool (real-time safe)
FileIoRequest *allocFileIoRequest();
void freeFileIoRequest( FileIoRequest *r );

// post a request to the server (real-time safe) 
void sendFileIoRequestToServer( FileIoRequest *r );

#endif /* INCLUDED_STREAMINGFILEIO_H */