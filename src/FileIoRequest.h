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
#ifndef INCLUDED_FILEIOREQUEST_H
#define INCLUDED_FILEIOREQUEST_H

#include <cstdlib> // size_t

#include "QwSpscUnorderedResultQueue.h"
#include "SharedBuffer.h"

#define IO_INVALID_FILE_HANDLE (0)

struct DataBlock;
struct QwSharedBuffer;

struct FileIoRequest{
    enum LinkIndices{
        TRANSIT_NEXT_LINK_INDEX = 0,
        CLIENT_NEXT_LINK_INDEX = 1,
        LINK_COUNT=2
    };
    FileIoRequest* links_[LINK_COUNT];

    enum RequestType {
        OPEN_FILE,
        CLOSE_FILE,
        READ_BLOCK,
        RELEASE_READ_BLOCK,
        ALLOCATE_WRITE_BLOCK,
        COMMIT_MODIFIED_WRITE_BLOCK,
        RELEASE_UNMODIFIED_WRITE_BLOCK,
        CLEANUP_RESULT_QUEUE,
        RESULT_QUEUE_IS_AWAITING_CLEANUP_, /* SERVER INTERNAL USE ONLY */

        CLIENT_USE_BASE_ = 16
    };

    enum OpenMode {
        READ_ONLY_OPEN_MODE,
        READ_WRITE_OVERWRITE_OPEN_MODE
    };

    int resultStatus; // an ERRNO value

    union {
        size_t clientInt;
        void *clientPtr;
    };

    // result queue type, used below:
    typedef QwSpscUnorderedResultQueue<FileIoRequest*,TRANSIT_NEXT_LINK_INDEX> result_queue_t;
    
    // discriminated union:
    int requestType; // RequestType
    union {

        /* OPEN_FILE */ 
        struct {
            SharedBuffer *path;         // IN NOTE: request owns a ref to path (use addRef and release)
            OpenMode openMode;          // IN
            void *fileHandle;           // OUT
            FileIoRequest *resultQueue; // IN
        } openFile;

        /* CLOSE_FILE */ 
        struct {
            void *fileHandle;           // IN
        } closeFile;

        /* READ_BLOCK */ 
        struct {
            void *fileHandle;           // IN
            std::size_t filePosition;   // IN
            DataBlock *dataBlock;       // OUT
            bool isAtEof;               // OUT
            FileIoRequest *resultQueue; // IN
        } readBlock;

        /* RELEASE_READ_BLOCK */ 
        struct {
            void *fileHandle;           // IN
            DataBlock *dataBlock;       // IN
        } releaseReadBlock;

        /* ALLOCATE_WRITE_BLOCK */ 
        struct {
            void *fileHandle;           // IN
            std::size_t filePosition;   // IN
            DataBlock *dataBlock;       // OUT
            FileIoRequest *resultQueue; // IN
        } allocateWriteBlock;

        /* COMMIT_MODIFIED_WRITE_BLOCK */ 
        struct {
            void *fileHandle;           // IN
            std::size_t filePosition;   // IN
            DataBlock *dataBlock;       // IN
        } commitModifiedWriteBlock;

        /* RELEASE_UNMODIFIED_WRITE_BLOCK */ 
        struct {
            void *fileHandle;           // IN
            DataBlock *dataBlock;       // IN
        } releaseUnmodifiedWriteBlock;

        /* CLEANUP_RESULT_QUEUE */
        result_queue_t resultQueue;
    };
};

#endif /* INCLUDED_FILEIOREQUEST_H */
