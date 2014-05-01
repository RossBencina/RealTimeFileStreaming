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
#include "FileIoReadStream.h"

#undef min
#undef max
#include <algorithm>
#include <cassert>

#include "FileIoServer.h"
#include "QwSPSCUnorderedResultQueue.h"
#include "SharedBuffer.h"
#include "DataBlock.h"


class FileIoReadStreamWrapper { // Object-oriented wrapper for a read stream

    FileIoRequest *resultQueueReq_; // The data structure is represented by a linked structure of FileIoRequest objects

    /*
        The stream data structure is composed of linked request nodes.
        OpenFileReq is linked by the result queue's transit link. This works because
        the transit link is not used unless the RQ is posted to the server for cleanup.

             READSTREAM*
                 |
                 | (resultQueueReq_)
                 V 
         [ result queue ] -> [ OPEN_FILE ] ------------------
                 |         (openFileReq)                    |
          (head) |                                   (tail) | 
                 V                                          V
       {  [ READ_BLOCK ] -> [ READ_BLOCK ] -> ... -> [ READ_BLOCK ] -> NULL  } (prefetchQueue)


       [ ... ] indicates a FileIoRequest structure.
    */

    // Stream field lvalue aliases. Map/alias request fields to fields of our pseudo-class.

    FileIoRequest*& openFileReqLink_() { return resultQueueReq_->links_[FileIoRequest::TRANSIT_NEXT_LINK_INDEX]; }
    FileIoRequest* openFileReq() { return openFileReqLink_(); }
    int& state_() { return resultQueueReq_->requestType; }
    int& error_() { return resultQueueReq_->resultStatus; }
    FileIoRequest*& prefetchQueueHead_() { return resultQueueReq_->links_[FileIoRequest::CLIENT_NEXT_LINK_INDEX]; }
    FileIoRequest*& prefetchQueueTail_() { return openFileReq()->links_[FileIoRequest::CLIENT_NEXT_LINK_INDEX]; }
    size_t& waitingForBlocksCount_() { return resultQueueReq_->clientInt; }
    FileIoRequest::result_queue_t& resultQueue() { return resultQueueReq_->resultQueue; }

    // Data block request field lvalue aliases. Map/alias request fields to our use of them.

    FileIoRequest*& blockReq_next_(FileIoRequest *r) { return r->links_[FileIoRequest::CLIENT_NEXT_LINK_INDEX]; }
    int& blockReq_state_(FileIoRequest *r) { return r->requestType; }
    size_t& blockReq_bytesCopied_(FileIoRequest *r) { return r->clientInt; }
    bool blockReq_isDiscarded(FileIoRequest *r) { return blockReq_bytesCopied_(r)==-1; }
    void blockReq_setDiscarded(FileIoRequest *r) { blockReq_bytesCopied_(r)=-1; }

    // Block request state:

    enum ReadStreamBlockState {
        READSTREAM_BLOCK_PENDING = FileIoRequest::READ_BLOCK,
        READSTREAM_BLOCK_READY = FileIoRequest::CLIENT_USE_BASE_,
        READSTREAM_BLOCK_ERROR
    };
    

    FileIoReadStreamWrapper( FileIoRequest *resultQueueReq )
        : resultQueueReq_( resultQueueReq ) {}

    void initReadBlockRequest( FileIoRequest *readBlockReq, size_t pos )
    {
        blockReq_next_(readBlockReq) = 0;
        readBlockReq->resultStatus = 0;
        blockReq_bytesCopied_(readBlockReq) = 0;
        readBlockReq->requestType = FileIoRequest::READ_BLOCK;

        readBlockReq->readBlock.fileHandle = openFileReq()->openFile.fileHandle;
        readBlockReq->readBlock.filePosition = pos;
        readBlockReq->readBlock.dataBlock = 0;
        readBlockReq->readBlock.resultQueue = resultQueueReq_;
    }

    FileIoRequest* prefetchQueue_front()
    {
        return prefetchQueueHead_();
    }

    void prefetchQueue_pop_front()
    {
        FileIoRequest *x = prefetchQueueHead_();
        prefetchQueueHead_() = blockReq_next_(x);
        blockReq_next_(x) = 0;
    }

    void prefetchQueue_push_back( FileIoRequest *readBlockReq )
    {
        assert( prefetchQueueTail_() != 0 ); // doesn't deal with an empty queue, doesn't need to
        blockReq_next_(prefetchQueueTail_()) = readBlockReq;
        prefetchQueueTail_() = readBlockReq;
    }

    void initLinkAndSendSequentialReadBlockRequest( FileIoRequest *readBlockReq )
    {
        // Init, link, and send a sequential READ_BLOCK request.
        // Init a READ_BLOCK request for the block following the latest block in the prefetch queue;
        // link the request onto the back of the prefetch queue; send the request to the server.

        // precondition: prefetch queue is non-empty
        assert( prefetchQueueHead_() !=0 && prefetchQueueTail_() !=0 );

        initReadBlockRequest( readBlockReq,
                prefetchQueueTail_()->readBlock.filePosition + IO_DATA_BLOCK_DATA_CAPACITY_BYTES );

        prefetchQueue_push_back( readBlockReq );

        // send READ_BLOCK request to server
        ::sendFileIoRequestToServer( readBlockReq );
        resultQueue().incrementExpectedResultCount();
        ++waitingForBlocksCount_();
    }

    static void releaseReadBlockToServer( FileIoRequest *blockReq )
    {
        void *fileHandle = blockReq->readBlock.fileHandle;
        DataBlock *dataBlock = blockReq->readBlock.dataBlock;

        blockReq->requestType = FileIoRequest::RELEASE_READ_BLOCK;
        blockReq->releaseReadBlock.fileHandle = fileHandle;
        blockReq->releaseReadBlock.dataBlock = dataBlock;
        ::sendFileIoRequestToServer( blockReq );
    }

    void flushReadBlock( FileIoRequest *blockReq )
    {
        switch (blockReq_state_(blockReq))
        {
        case READSTREAM_BLOCK_PENDING:
            // Forget the block, it will show up in the result queue later, 
            // and will be cleaned up from there.

            // Setting the "discarded" flag is used when the stream is still alive and we 
            // need to remove a pending request from the prefetch queue. 
            // See receiveOneBlock() for discarded block handling.
            blockReq_setDiscarded(blockReq);

            --waitingForBlocksCount_();
            break;

        case READSTREAM_BLOCK_READY:
            assert( blockReq->readBlock.dataBlock != 0 );
            releaseReadBlockToServer( blockReq );
            break;

        case READSTREAM_BLOCK_ERROR:
            assert( blockReq->readBlock.dataBlock == 0 );
            freeFileIoRequest( blockReq );
            break;
        }
    }

    void flushPrefetchQueue()
    {
        // for each block in the prefetch queue, pop the block from the head of the queue and clean up the block
        while (prefetchQueueHead_()) {
            FileIoRequest *blockReq = prefetchQueue_front();
            prefetchQueue_pop_front();
            flushReadBlock( blockReq );
        }

        prefetchQueueTail_() = 0;
        assert( waitingForBlocksCount_() == 0 );
    }

    static size_t roundDownToBlockSizeAlignedPosition( size_t pos )
    {
        size_t blockNumber = pos / IO_DATA_BLOCK_DATA_CAPACITY_BYTES;
        return blockNumber * IO_DATA_BLOCK_DATA_CAPACITY_BYTES;
    }

    // should only be called after the stream has been opened and before it is closed
    bool receiveOneBlock( )
    {
        if (FileIoRequest *r=resultQueue().pop())
        {
            assert( r->requestType == FileIoRequest::READ_BLOCK );

            if (blockReq_isDiscarded(r))
            {
                // the block was discarded. i.e. is no longer in the prefetch block queue

                if (r->resultStatus==NOERROR)
                {
                    releaseReadBlockToServer( r );
                }
                else
                {
                    // errors on discarded blocks don't affect the stream state
                    assert( r->readBlock.dataBlock == 0 );
                    freeFileIoRequest( r );
                }

                // NOTE: discarded blocks do not count against FileIoReadStream_waitingForBlocksCount_
            }
            else
            {
                if (--waitingForBlocksCount_() == 0)
                    state_() = READSTREAM_STATE_OPEN_STREAMING;

                if (r->resultStatus==NOERROR)
                {
                    assert( r->readBlock.dataBlock );
                    blockReq_state_(r) = READSTREAM_BLOCK_READY;
                }
                else
                {
                    // the block is marked as an error. the stream state will switch to error when the client tries to read the block
                    blockReq_state_(r) = READSTREAM_BLOCK_ERROR;                
                }
            }

            return true;
        }

        return false;
    }

public:
    FileIoReadStreamWrapper( READSTREAM *fp )
        : resultQueueReq_( static_cast<FileIoRequest*>(fp) ) {}

    READSTREAM* handle() { return resultQueueReq_; }


    static READSTREAM* open( SharedBuffer *path, FileIoRequest::OpenMode openMode )
    {
        // Allocate two requests. Return 0 if allocation fails.

        FileIoRequest *resultQueueReq = allocFileIoRequest();
        if (!resultQueueReq)
            return 0;

        FileIoRequest *openFileReq = allocFileIoRequest();
        if (!openFileReq) {
            freeFileIoRequest(resultQueueReq);
            return 0;
        }

        // Initialise the stream data structure

        FileIoReadStreamWrapper stream(resultQueueReq);
        //std::memset( &resultQueueReq, 0, sizeof(FileIoRequest) );
        stream.resultQueue().init();

        stream.openFileReqLink_() = openFileReq;
        stream.state_() = READSTREAM_STATE_OPENING;
        stream.error_() = 0;
        stream.prefetchQueueHead_() = 0;
        stream.prefetchQueueTail_() = 0;
        stream.waitingForBlocksCount_() = 0;
        
        // Issue the OPEN_FILE request

        openFileReq->resultStatus = 0;
        openFileReq->requestType = FileIoRequest::OPEN_FILE;
        path->addRef();
        openFileReq->openFile.path = path;
        openFileReq->openFile.openMode = openMode;
        openFileReq->openFile.fileHandle = IO_INVALID_FILE_HANDLE;
        openFileReq->openFile.resultQueue = resultQueueReq;

        ::sendFileIoRequestToServer( openFileReq );
        stream.resultQueue().incrementExpectedResultCount();

        return stream.handle();
    }

    void close()
    {
        // (Don't poll state, just dispose current state)

        if (state_()==READSTREAM_STATE_OPENING)
        {
            // Still waiting for OPEN_FILE to return. Send the result queue to the server for cleanup.
            openFileReqLink_() = 0;

            resultQueueReq_->requestType = FileIoRequest::CLEANUP_RESULT_QUEUE;
            ::sendFileIoRequestToServer( resultQueueReq_ );
            resultQueueReq_ = 0;
        }
        else
        {
            // Stream is open. The prefetch queue may contain requests.

            // Dispose the prefetch queue, if it's populated

            flushPrefetchQueue();

            // Clean up the open file request

            {
                FileIoRequest *openFileReq = openFileReqLink_();
                openFileReqLink_() = 0;
                if (openFileReq->openFile.fileHandle != IO_INVALID_FILE_HANDLE)
                {
                    // Transform openFileReq to CLOSE_FILE and send to server
                    void *fileHandle = openFileReq->openFile.fileHandle;
                    
                    FileIoRequest *closeFileReq = openFileReq;
                    closeFileReq->requestType = FileIoRequest::CLOSE_FILE;
                    closeFileReq->closeFile.fileHandle = fileHandle;
                    ::sendFileIoRequestToServer( closeFileReq );
                }
                else
                {
                    freeFileIoRequest( openFileReq );
                }
            }

            // Clean up the result queue

            if (resultQueue().expectedResultCount() > 0)
            {
                // Send the result queue to the server for cleanup
                resultQueueReq_->requestType = FileIoRequest::CLEANUP_RESULT_QUEUE;
                ::sendFileIoRequestToServer( resultQueueReq_ );
                resultQueueReq_ = 0;
            }
            else
            {
                freeFileIoRequest( resultQueueReq_ );
                resultQueueReq_ = 0;
            }
        }
    }

    int seek( size_t pos )
    {
        assert( pos >= 0 );

        if (state_() == READSTREAM_STATE_OPENING || state_() == READSTREAM_STATE_ERROR)
            return -1;

        // Straight-forward implementation of seek: dump all blocks from the prefetch queue, 
        // then request the needed blocks.
        // A more optimised version would retain any needed blocks from the current prefetch queue.

        // FIXME HACK: hardcode prefetch queue length. 
        // The prefetch queue length should be computed from the stream data rate and the 
        // desired prefetch buffering length (in seconds).
        flushPrefetchQueue();

        const int prefetchQueueBlockCount = 20;

        // Request blocks on block-size-aligned boundaries
        size_t blockFilePositionBytes = roundDownToBlockSizeAlignedPosition(pos);

        // request the first block 

        // FIXME TODO: queue all requests at once with enqueue-multiple
        // Definitely use push-multiple when performing a seek operation. That minimises contention.
        // TODO: also use queue multiple for closing the stream

        FileIoRequest *firstBlockReq = allocFileIoRequest();
        if (!firstBlockReq) {
            state_() = READSTREAM_STATE_ERROR;
            return -1;
        }

        initReadBlockRequest( firstBlockReq, blockFilePositionBytes );

        blockReq_bytesCopied_(firstBlockReq) = pos - blockFilePositionBytes; // compensate for block-size-aligned request
        prefetchQueueHead_() = firstBlockReq;
        prefetchQueueTail_() = firstBlockReq;
        ::sendFileIoRequestToServer(firstBlockReq);
        resultQueue().incrementExpectedResultCount();
        ++waitingForBlocksCount_();

        for (int i=1; i < prefetchQueueBlockCount; ++i) {
            FileIoRequest *readBlockReq = allocFileIoRequest();
            if (!readBlockReq) {
                // fail. couldn't allocate request
                state_() = READSTREAM_STATE_ERROR;
                return -1;
            }

            initLinkAndSendSequentialReadBlockRequest( readBlockReq );
        }

        state_() = READSTREAM_STATE_OPEN_BUFFERING;

        return 0;
    }

    size_t read( void *dest, size_t itemSize, size_t itemCount )
    {
        pollState(); // always process at least one expected reply per read call

        switch (state_())
        {
        case READSTREAM_STATE_OPENING:
            return 0;
            break;

        case READSTREAM_STATE_OPEN_IDLE:
        case READSTREAM_STATE_OPEN_EOF:
            return 0;
            break;

        case READSTREAM_STATE_OPEN_BUFFERING:
        case READSTREAM_STATE_OPEN_STREAMING:
            {
                int8_t *destBytesPtr = (int8_t*)dest;
                size_t totalBytesToCopyToDest = itemSize * itemCount;
                size_t bytesCopiedToDestSoFar = 0;

                while (bytesCopiedToDestSoFar < totalBytesToCopyToDest)
                {
                    FileIoRequest *frontBlockReq = prefetchQueue_front();
                    assert( frontBlockReq != 0 );

#if 0
                    // Last-ditch effort to determine whether the front block has been returned.
                    // O(n) in the maximum number of expected replies.
                    // Since we always poll at least one block per read() operation (call to 
                    // FileIoReadStream_pollState above), the following loop is not strictly necessary, 
                    // it serves two purposes: (1) it reduces the latency of transitioning from 
                    // BUFFERING to STREAMING, (2) it lessens the likelihood of a buffer under-run. 

                    // process replies until the block is not pending or there are no more replies
                    while (blockReq_state_(frontBlockReq) == READSTREAM_BLOCK_PENDING) {
                        if (!receiveOneBlock())
                            break;
                    }
#endif

                    if (blockReq_state_(frontBlockReq) == READSTREAM_BLOCK_READY)
                    {
                        // copy data to dest

                        size_t bytesCopiedFromBlockSoFar = blockReq_bytesCopied_(frontBlockReq);
                        size_t bytesRemainingInBlock = frontBlockReq->readBlock.dataBlock->validCountBytes - bytesCopiedFromBlockSoFar;
                        size_t bytesRemainingToCopyToDest = totalBytesToCopyToDest - bytesCopiedToDestSoFar;
                        size_t n = std::min<size_t>( bytesRemainingInBlock, bytesRemainingToCopyToDest );

                        std::memcpy(destBytesPtr, static_cast<int8_t*>(frontBlockReq->readBlock.dataBlock->data)+bytesCopiedFromBlockSoFar, n);

                        bytesCopiedToDestSoFar += n;
                        bytesCopiedFromBlockSoFar += n;
                        bytesRemainingInBlock -= n;
                        assert( bytesRemainingInBlock == 0 || bytesRemainingInBlock >= itemSize ); // HACK: assume that itemSize divides block size, otherwise we need to deal with items overlapping blocks
                        blockReq_bytesCopied_(frontBlockReq) = bytesCopiedFromBlockSoFar;

                        if (bytesRemainingInBlock==0)
                        {
                            if (frontBlockReq->readBlock.isAtEof) {

                                state_() = READSTREAM_STATE_OPEN_EOF;
                                return bytesCopiedToDestSoFar / itemSize;
                            }
                            else
                            {
                                // request and link the next block...
                                FileIoRequest *readBlockReq = allocFileIoRequest();
                                if (!readBlockReq) {
                                    // fail. couldn't allocate request
                                    state_() = READSTREAM_STATE_ERROR;
                                    return bytesCopiedToDestSoFar / itemSize;
                                }

                                // issue next block request, link it on to the tail of the prefetch queue
                                initLinkAndSendSequentialReadBlockRequest( readBlockReq );

                                // unlink the old block...

                                // Notice that we link the new request on the back of the prefetch queue before unlinking 
                                // the old one off the front, so there is no chance of having to deal with the special case of 
                                // linking to an empty queue.

                                prefetchQueue_pop_front(); // advance head to next block
                                flushReadBlock( frontBlockReq ); // send the old block back to the server
                            }
                        }
                    }
                    else
                    {
                        if (blockReq_state_(frontBlockReq) == READSTREAM_BLOCK_ERROR)
                            state_() = READSTREAM_STATE_ERROR;
                        else
                            state_() = READSTREAM_STATE_OPEN_BUFFERING;

                        // head block is pending, or we've entered the error state
                        return bytesCopiedToDestSoFar / itemSize;
                    }
                }

                return itemCount;
            }
            break;

        case READSTREAM_STATE_ERROR:
            return 0;
            break;
        }

        return 0;
    }

    FileIoReadStreamState pollState()
    {
        if (resultQueue().expectedResultCount() > 0)
        {
            if (state_()==READSTREAM_STATE_OPENING)
            {
                if (FileIoRequest *r=resultQueue().pop())
                {
                    assert( r == openFileReq() ); // when opening, the only possible result is the open file request
                    
                    r->openFile.path->release();
                    r->openFile.path = 0;

                    if (r->resultStatus==NOERROR)
                    {
                        assert( r->openFile.fileHandle != 0 );

                        state_() = READSTREAM_STATE_OPEN_IDLE;
                        // NOTE: in principle we could seek here. at the moment we require the client to poll for idle.
                    }
                    else
                    {
                        error_() = r->resultStatus;
                        state_() = READSTREAM_STATE_ERROR;
                    }

                    // Leave openFileReq linked to the structure, even if there's an error
                }
            }
            else
            {
                assert( state_()==READSTREAM_STATE_OPEN_IDLE 
                    || state_()==READSTREAM_STATE_OPEN_EOF
                    || state_()==READSTREAM_STATE_OPEN_BUFFERING 
                    || state_()==READSTREAM_STATE_OPEN_STREAMING
                    || state_()==READSTREAM_STATE_ERROR );

                receiveOneBlock();
            }
        }

        return (FileIoReadStreamState)state_();
    }

    int getError()
    {
        return error_();
    }
};


READSTREAM *FileIoReadStream_open( SharedBuffer *path, FileIoRequest::OpenMode openMode )
{
    return FileIoReadStreamWrapper::open(path, openMode);
}

void FileIoReadStream_close( READSTREAM *fp )
{
    FileIoReadStreamWrapper(fp).close();
}

int FileIoReadStream_seek( READSTREAM *fp, size_t pos )
{
    return FileIoReadStreamWrapper(fp).seek(pos);
}

size_t FileIoReadStream_read( void *dest, size_t itemSize, size_t itemCount, READSTREAM *fp )
{
    return FileIoReadStreamWrapper(fp).read(dest, itemSize, itemCount);
}

FileIoReadStreamState FileIoReadStream_pollState( READSTREAM *fp )
{
    return FileIoReadStreamWrapper(fp).pollState();
}

int FileIoReadStream_getError( READSTREAM *fp )
{
    return FileIoReadStreamWrapper(fp).getError();
}


void FileIoReadStream_test()
{
    printf( "> FileIoReadStream_test()\n" );

    printf( "opening " );

    // in msvc this is a path relative to the project directory:
    SharedBuffer *path = SharedBufferAllocator::alloc("..\\..\\..\\src\\FileIoReadStream.cpp"); // print out the source code of this file
    READSTREAM *fp = FileIoReadStream_open(path, FileIoRequest::READ_ONLY_OPEN_MODE);
    path->release();
    assert( fp != 0 );

    while (FileIoReadStream_pollState(fp) == READSTREAM_STATE_OPENING) {
        printf(".");
        Sleep(10);
    }

    printf( "\ndone.\n" );

    assert( FileIoReadStream_pollState(fp) == READSTREAM_STATE_OPEN_IDLE );

    printf( "seeking " );

    FileIoReadStream_seek(fp, 0);

    while (FileIoReadStream_pollState(fp) == READSTREAM_STATE_OPEN_BUFFERING) {
        printf(".");
        Sleep(10);
    }
    
    assert( FileIoReadStream_pollState(fp) == READSTREAM_STATE_OPEN_STREAMING );

    printf( "\ndone.\n" );

    printf( "reading:\n" );

    while (FileIoReadStream_pollState(fp) == READSTREAM_STATE_OPEN_STREAMING || FileIoReadStream_pollState(fp) == READSTREAM_STATE_OPEN_BUFFERING) {

        /*
        // make sure we're always streaming:
        while (FileIoReadStream_pollState(fp) == READSTREAM_STATE_OPEN_BUFFERING) {
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

    assert( FileIoReadStream_pollState(fp) == READSTREAM_STATE_OPEN_EOF );

    printf( "\nclosing.\n" );

    FileIoReadStream_close(fp);

    printf( "< FileIoReadStream_test()\n" );
}
