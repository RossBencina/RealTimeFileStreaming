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


// The stream data structure is composed out of a linked structure of request nodes.
// These macros provide access to the fields.
// openFileReq is linked by the result queue's transit link. This works because
// the transit link is not used unless the RQ is posted to the server for cleanup.

// macros that apply to the stream:

#define FileIoReadStream_openFileReqLink_             (resultQueueReq->links_[FileIoRequest::TRANSIT_NEXT_LINK_INDEX])

#define FileIoReadStream_state_                       (resultQueueReq->requestType)
#define FileIoReadStream_error_                       (resultQueueReq->resultStatus)

#define FileIoReadStream_blocks_head_                 (resultQueueReq->links_[FileIoRequest::CLIENT_NEXT_LINK_INDEX])
#define FileIoReadStream_blocks_tail_                 (openFileReq->links_[FileIoRequest::CLIENT_NEXT_LINK_INDEX])
#define FileIoReadStream_waitingForBlocksCount_       (resultQueueReq->clientInt)


// macros that apply to blocks

enum ReadStreamBlockState {
    READSTREAM_BLOCK_PENDING = FileIoRequest::READ_BLOCK,
    READSTREAM_BLOCK_READY = FileIoRequest::CLIENT_USE_BASE_,
    READSTREAM_BLOCK_ERROR
};

#define FileIoReadStream_Block_next_(r)               (r->links_[FileIoRequest::CLIENT_NEXT_LINK_INDEX])
#define FileIoReadStream_Block_state_(r)              (r->requestType)
#define FileIoReadStream_Block_state_(r)              (r->requestType)

#define FileIoReadStream_Block_bytesCopied_(r)        (r->clientInt)
#define FileIoReadStream_Block_isDiscarded(r)         (FileIoReadStream_Block_bytesCopied_(r)==-1)
#define FileIoReadStream_Block_setDiscarded(r)        (FileIoReadStream_Block_bytesCopied_(r)=-1)


READSTREAM *FileIoReadStream_open( SharedBuffer *path, FileIoRequest::OpenMode openMode )
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

    // Initialise the stream data structure.

    //std::memset( &resultQueueReq, 0, sizeof(FileIoRequest) );
    resultQueueReq->resultQueue.init();
            
    FileIoReadStream_openFileReqLink_ = openFileReq;

    FileIoReadStream_state_ = READSTREAM_STATE_OPENING;
    FileIoReadStream_error_ = 0;
    FileIoReadStream_blocks_head_ = 0;
    FileIoReadStream_blocks_tail_ = 0;
    FileIoReadStream_waitingForBlocksCount_ = 0;

    // Issue the OPEN_FILE request.

    openFileReq->resultStatus = 0;
    openFileReq->requestType = FileIoRequest::OPEN_FILE;
    path->addRef();
    openFileReq->openFile.path = path;
    openFileReq->openFile.openMode = openMode;
    openFileReq->openFile.fileHandle = IO_INVALID_FILE_HANDLE;
    openFileReq->openFile.resultQueue = resultQueueReq;

    sendFileIoRequestToServer( openFileReq );
    resultQueueReq->resultQueue.incrementExpectedResultCount();
    
    return (READSTREAM*)resultQueueReq;
}


static void initReadBlockRequest( FileIoRequest *resultQueueReq, FileIoRequest *openFileReq, FileIoRequest *readBlockReq, size_t pos )
{
    FileIoReadStream_Block_next_(readBlockReq) = 0;
    readBlockReq->resultStatus = 0;
    FileIoReadStream_Block_bytesCopied_(readBlockReq) = 0;
    readBlockReq->requestType = FileIoRequest::READ_BLOCK;

    readBlockReq->readBlock.fileHandle = openFileReq->openFile.fileHandle;
    readBlockReq->readBlock.filePosition = pos;
    readBlockReq->readBlock.dataBlock = 0;
    readBlockReq->readBlock.resultQueue = resultQueueReq;
}

static void initLinkAndSendSequentialReadBlockRequest( FileIoRequest *resultQueueReq, FileIoRequest *openFileReq, FileIoRequest *readBlockReq )
{
    // Init, link, and send a sequential READ_BLOCK request.
    // Init a READ_BLOCK request for the block following the latest block in the prefetch queue;
    // link the request onto the back of the prefetch queue; send the request to the server.
    
    // precondition: prefetch queue is non-empty
    assert( FileIoReadStream_blocks_head_!=0 && FileIoReadStream_blocks_tail_!=0 );

    initReadBlockRequest( resultQueueReq, openFileReq, readBlockReq,
            FileIoReadStream_blocks_tail_->readBlock.filePosition + IO_DATA_BLOCK_DATA_CAPACITY_BYTES );

    // link newReadBlockReq on to tail
    FileIoReadStream_Block_next_(FileIoReadStream_blocks_tail_) = readBlockReq;
    FileIoReadStream_blocks_tail_ = readBlockReq;

    // send READ_BLOCK request to server
    sendFileIoRequestToServer( readBlockReq );
    resultQueueReq->resultQueue.incrementExpectedResultCount();
    ++FileIoReadStream_waitingForBlocksCount_;
}

static void releaseReadBlockToServer( FileIoRequest *blockReq )
{
    void *fileHandle = blockReq->readBlock.fileHandle;
    DataBlock *dataBlock = blockReq->readBlock.dataBlock;

    blockReq->requestType = FileIoRequest::RELEASE_READ_BLOCK;
    blockReq->releaseReadBlock.fileHandle = fileHandle;
    blockReq->releaseReadBlock.dataBlock = dataBlock;
    sendFileIoRequestToServer( blockReq );
    blockReq = 0;
}


static void flushReadBlock( FileIoRequest *resultQueueReq, FileIoRequest *blockReq )
{
    switch (FileIoReadStream_Block_state_(blockReq))
    {
    case READSTREAM_BLOCK_PENDING:
        // Forget the block, it will show up in the result queue later, 
        // and will be cleaned up from there.

        // Setting the "discarded" flag is used when the stream is still alive and we 
        // need to remove a pending request from the prefetch queue. 
        // See receiveOneBlock() for discarded block handling.
        FileIoReadStream_Block_setDiscarded(blockReq);

        --FileIoReadStream_waitingForBlocksCount_;
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


static void flushPrefetchQueue( FileIoRequest *resultQueueReq, FileIoRequest *openFileReq )
{
    // for each block in the prefetch queue, pop the block from the head of the queue and clean up the block
    while (FileIoReadStream_blocks_head_) {
        FileIoRequest *blockReq = FileIoReadStream_blocks_head_;
        FileIoReadStream_blocks_head_ = FileIoReadStream_Block_next_(blockReq);
        FileIoReadStream_Block_next_(blockReq) = 0;

        flushReadBlock( resultQueueReq, blockReq );
    }

    FileIoReadStream_blocks_tail_ = 0;

    assert( FileIoReadStream_waitingForBlocksCount_ == 0 );
}


void FileIoReadStream_close( READSTREAM *fp )
{
    FileIoRequest *resultQueueReq=(FileIoRequest*)fp;
    FileIoRequest *openFileReq=FileIoReadStream_openFileReqLink_;

    // don't poll state for close, just dispose current state

    if (FileIoReadStream_state_==READSTREAM_STATE_OPENING)
    {
        // Still waiting for OPEN_FILE to return. Send the result queue to the server for cleanup.
        FileIoReadStream_openFileReqLink_ = 0;
        openFileReq = 0;

        resultQueueReq->requestType = FileIoRequest::CLEANUP_RESULT_QUEUE;
        sendFileIoRequestToServer( resultQueueReq );
        resultQueueReq = 0;
    }
    else
    {
        // Stream is open. The prefetch queue may contain requests.

        // dispose the prefetch queue, if it's populated
        flushPrefetchQueue( resultQueueReq, openFileReq );

        // clean up the open file request

        if (openFileReq->openFile.fileHandle != IO_INVALID_FILE_HANDLE)
        {
            // transform openFileReq to CLOSE_FILE and send to server
            void *fileHandle = openFileReq->openFile.fileHandle;
            FileIoRequest *closeFileReq = openFileReq;
            openFileReq = 0;
            FileIoReadStream_openFileReqLink_ = 0;

            closeFileReq->requestType = FileIoRequest::CLOSE_FILE;
            closeFileReq->closeFile.fileHandle = fileHandle;
            sendFileIoRequestToServer( closeFileReq );
            closeFileReq = 0;
        }
        else
        {
            freeFileIoRequest( openFileReq );
            openFileReq = 0;
            FileIoReadStream_openFileReqLink_ = 0;
        }

        // clean up the result queue

        if (resultQueueReq->resultQueue.expectedResultCount() > 0)
        {
            // send the result queue to the server for cleanup
            resultQueueReq->requestType = FileIoRequest::CLEANUP_RESULT_QUEUE;
            sendFileIoRequestToServer( resultQueueReq );
            resultQueueReq = 0;
        }
        else
        {
            freeFileIoRequest( resultQueueReq );
            resultQueueReq = 0;
        }
    }
}


static size_t roundDownToBlockSizeAlignedPosition( size_t pos )
{
    size_t blockNumber = pos / IO_DATA_BLOCK_DATA_CAPACITY_BYTES;
    return blockNumber * IO_DATA_BLOCK_DATA_CAPACITY_BYTES;
}

int FileIoReadStream_seek( READSTREAM *fp, size_t pos )
{
    assert( pos >= 0 );

    FileIoRequest *resultQueueReq=(FileIoRequest*)fp;
    FileIoRequest *openFileReq=FileIoReadStream_openFileReqLink_;

    if (FileIoReadStream_state_ == READSTREAM_STATE_OPENING || FileIoReadStream_state_ == READSTREAM_STATE_ERROR)
        return -1;

    // Straight-forward implementation of seek: dump all blocks from the prefetch queue, 
    // then request the needed blocks.
    // A more optimised version would retain any needed blocks from the current prefetch queue.

    // FIXME HACK: hardcode prefetch queue length. 
    // The prefetch queue length should be computed from the stream data rate and the 
    // desired prefetch buffering length (in seconds).
    flushPrefetchQueue( resultQueueReq, openFileReq );

    const int prefetchQueueBlockCount = 20;

    // Request blocks on block-size-aligned boundaries
    size_t blockFilePositionBytes = roundDownToBlockSizeAlignedPosition(pos);

    // request the first block 

// FIXME TODO: queue all requests at once with enqueue-multiple
// Definitely use push-multiple when performing a seek operation. That minimises contention.
// TODO: also use queue multiple for closing the stream

    FileIoRequest *firstBlockReq = allocFileIoRequest();
    if (!firstBlockReq) {
        FileIoReadStream_state_ = READSTREAM_STATE_ERROR;
        return -1;
    }

    initReadBlockRequest( resultQueueReq, openFileReq, firstBlockReq, blockFilePositionBytes );

    FileIoReadStream_Block_bytesCopied_(firstBlockReq) = pos - blockFilePositionBytes; // compensate for block-size-aligned request
    FileIoReadStream_blocks_head_ = firstBlockReq;
    FileIoReadStream_blocks_tail_ = firstBlockReq;
    sendFileIoRequestToServer(firstBlockReq);
    resultQueueReq->resultQueue.incrementExpectedResultCount();
    ++FileIoReadStream_waitingForBlocksCount_;

    for (int i=1; i < prefetchQueueBlockCount; ++i) {
        FileIoRequest *readBlockReq = allocFileIoRequest();
        if (!readBlockReq) {
            // fail. couldn't allocate request
            FileIoReadStream_state_ = READSTREAM_STATE_ERROR;
            return -1;
        }

        initLinkAndSendSequentialReadBlockRequest( resultQueueReq, openFileReq, readBlockReq );
    }

    FileIoReadStream_state_ = READSTREAM_STATE_OPEN_BUFFERING;

    return 0;
}


// should only be called after the stream has been opened and before it is closed
static bool receiveOneBlock( FileIoRequest *resultQueueReq )
{
    if (FileIoRequest *r=resultQueueReq->resultQueue.pop())
    {
        assert( r->requestType == FileIoRequest::READ_BLOCK );

        if (FileIoReadStream_Block_isDiscarded(r))
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
            if (--FileIoReadStream_waitingForBlocksCount_ == 0)
                FileIoReadStream_state_ = READSTREAM_STATE_OPEN_STREAMING;

            if (r->resultStatus==NOERROR)
            {
                assert( r->readBlock.dataBlock );
                FileIoReadStream_Block_state_(r) = READSTREAM_BLOCK_READY;
            }
            else
            {
                // the block is marked as an error. the stream state will switch to error when the client tries to read the block
                FileIoReadStream_Block_state_(r) = READSTREAM_BLOCK_ERROR;                
            }
        }

        return true;
    }

    return false;
}


size_t FileIoReadStream_read( void *dest, size_t itemSize, size_t itemCount, READSTREAM *fp )
{
    FileIoRequest *resultQueueReq=(FileIoRequest*)fp;
    FileIoRequest *openFileReq=FileIoReadStream_openFileReqLink_;

    FileIoReadStream_pollState(fp); // always process at least one expected reply per read call

    switch (FileIoReadStream_state_)
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
                assert( FileIoReadStream_blocks_head_ != 0 );
                FileIoRequest *headBlockReq = FileIoReadStream_blocks_head_;

#if 0
                // Last-ditch effort to determine whether the head block has been returned.
                // O(n) in the maximum number of expected replies.
                // Since we always poll at least one block per read() operation (call to 
                // FileIoReadStream_pollState above), the following loop is not strictly necessary, 
                // it serves two purposes: (1) it reduces the latency of transitioning from 
                // BUFFERING to STREAMING, (2) it lessens the likelihood of a buffer under-run. 

                // process replies until the block is not pending or there are no more replies
                while (FileIoReadStream_Block_state_(headBlockReq) == READSTREAM_BLOCK_PENDING) {
                    if (!receiveOneBlock(resultQueueReq))
                        break;
                }
#endif

                if (FileIoReadStream_Block_state_(headBlockReq) == READSTREAM_BLOCK_READY)
                {
                    // copy data to dest
                    
                    size_t bytesCopiedFromBlockSoFar = FileIoReadStream_Block_bytesCopied_(headBlockReq);
                    size_t bytesRemainingInBlock = headBlockReq->readBlock.dataBlock->validCountBytes - bytesCopiedFromBlockSoFar;
                    size_t bytesRemainingToCopyToDest = totalBytesToCopyToDest - bytesCopiedToDestSoFar;
                    size_t n = std::min<size_t>( bytesRemainingInBlock, bytesRemainingToCopyToDest );

                    std::memcpy(destBytesPtr, static_cast<int8_t*>(headBlockReq->readBlock.dataBlock->data)+bytesCopiedFromBlockSoFar, n);

                    bytesCopiedToDestSoFar += n;
                    bytesCopiedFromBlockSoFar += n;
                    bytesRemainingInBlock -= n;
                    assert( bytesRemainingInBlock == 0 || bytesRemainingInBlock >= itemSize ); // HACK: assume that itemSize divides block size, otherwise we need to deal with items overlapping blocks
                    FileIoReadStream_Block_bytesCopied_(headBlockReq) = bytesCopiedFromBlockSoFar;
                                    
                    if (bytesRemainingInBlock==0)
                    {
                        if (headBlockReq->readBlock.isAtEof) {

                            FileIoReadStream_state_ = READSTREAM_STATE_OPEN_EOF;
                            return bytesCopiedToDestSoFar / itemSize;
                        }
                        else
                        {
                            // request and link the next block...
                            FileIoRequest *readBlockReq = allocFileIoRequest();
                            if (!readBlockReq) {
                                // fail. couldn't allocate request
                                FileIoReadStream_state_ = READSTREAM_STATE_ERROR;
                                return bytesCopiedToDestSoFar / itemSize;
                            }

                            // issue next block request, link it on to the tail of the prefetch queue
                            initLinkAndSendSequentialReadBlockRequest( resultQueueReq, openFileReq, readBlockReq );

                            // unlink the old block...

                            // Notice that since we link the new request on the back of the prefetch queue before unlinking 
                            // the old one off the front there is no chance of having to deal with the special case of 
                            // linking to an empty queue.

                            FileIoReadStream_blocks_head_ = FileIoReadStream_Block_next_(headBlockReq); // advance head to next block
                            flushReadBlock( resultQueueReq, headBlockReq ); // send the old block back to the server
                        }
                    }
                }
                else
                {
                    if (FileIoReadStream_Block_state_(headBlockReq) == READSTREAM_BLOCK_ERROR)
                        FileIoReadStream_state_ = READSTREAM_STATE_ERROR;
                    else
                        FileIoReadStream_state_ = READSTREAM_STATE_OPEN_BUFFERING;

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


FileIoReadStreamState FileIoReadStream_pollState( READSTREAM *fp )
{
    FileIoRequest *resultQueueReq=(FileIoRequest*)fp;
    FileIoRequest *openFileReq=FileIoReadStream_openFileReqLink_;

    if (resultQueueReq->resultQueue.expectedResultCount() > 0)
    {
        if (FileIoReadStream_state_==READSTREAM_STATE_OPENING)
        {
            if (FileIoRequest *r=resultQueueReq->resultQueue.pop())
            {
                assert( r == openFileReq ); // when opening, the only possible result is the open file request
                r = 0;

                openFileReq->openFile.path->release();
                openFileReq->openFile.path = 0;

                if (openFileReq->resultStatus==NOERROR)
                {
                    assert( openFileReq->openFile.fileHandle != 0 );

                    FileIoReadStream_state_ = READSTREAM_STATE_OPEN_IDLE;
                    // NOTE: in principle we could seek here. at the moment we require the client to poll for idle.
                }
                else
                {
                    FileIoReadStream_error_ = openFileReq->resultStatus;
                    FileIoReadStream_state_ = READSTREAM_STATE_ERROR;
                }
            }
        }
        else
        {
            assert( FileIoReadStream_state_==READSTREAM_STATE_OPEN_IDLE 
                    || FileIoReadStream_state_==READSTREAM_STATE_OPEN_EOF
                    || FileIoReadStream_state_==READSTREAM_STATE_OPEN_BUFFERING 
                    || FileIoReadStream_state_==READSTREAM_STATE_OPEN_STREAMING
                    || FileIoReadStream_state_==READSTREAM_STATE_ERROR );

            receiveOneBlock( resultQueueReq );
        }
    }

    return (FileIoReadStreamState)FileIoReadStream_state_;
}


int FileIoReadStream_getError( READSTREAM *fp )
{
    FileIoRequest *resultQueueReq=(FileIoRequest*)fp;

    return FileIoReadStream_error_;
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
