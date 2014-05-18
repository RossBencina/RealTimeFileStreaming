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
#include "FileIoStreams.h"

#undef min
#undef max
#include <algorithm>
#include <cassert>
#include <functional>

#include "QwSpscUnorderedResultQueue.h"
#include "QwSList.h"
#include "FileIoServer.h"
#include "SharedBuffer.h"
#include "DataBlock.h"

#ifndef NOERROR
#define NOERROR (0)
#endif

//#define IO_USE_CONSTANT_TIME_RESULT_POLLING


struct BlockRequestBehavior { // behavior with implementation common to read and write requests

    // L-value aliases. Map/alias request fields to our use of them.

    static FileIoRequest*& next_(FileIoRequest *r) { return r->links_[FileIoRequest::CLIENT_NEXT_LINK_INDEX]; }
    static FileIoRequest*& transitNext_(FileIoRequest *r) { return r->links_[FileIoRequest::TRANSIT_NEXT_LINK_INDEX]; }
    static int& state_(FileIoRequest *r) { return r->requestType; }
    static size_t& bytesCopied_(FileIoRequest *r) { return r->clientInt; }
};


struct ReadBlockRequestBehavior : public BlockRequestBehavior {

    // when not in-flight, the request type field represents the state of the block request.
    // the acquire request type is mapped to the PENDING state.
    enum BlockState {
        BLOCK_STATE_PENDING = FileIoRequest::READ_BLOCK,
        BLOCK_STATE_READY = FileIoRequest::RELEASE_READ_BLOCK,
        BLOCK_STATE_READY_MODIFIED = FileIoRequest::CLIENT_USE_BASE_, // not used for read streams
        BLOCK_STATE_ERROR = FileIoRequest::NO_OP
    };

    // fields and predicates

    static DataBlock *dataBlock(FileIoRequest *r) { return r->releaseReadBlock.dataBlock; }
    static size_t filePosition(FileIoRequest *r) { return r->releaseReadBlock.filePosition; }
    
    static bool isReady(FileIoRequest *r) { return (state_(r) == BLOCK_STATE_READY); }
    
    // request initialization and transformation

    // In abstract terms, data blocks are /acquired/ from the server and /released/ back to the server.
    //
    // For read-only streams READ_BLOCK is acquire and RELEASE_READ_BLOCK is release.
    //
    // For write-only streams ALLOCATE_WRITE_BLOCK is acquire and
    // (RELEASE_UNMODIFIED_WRITE_BLOCK or COMMIT_MODIFIED_WRITE_BLOCK) is release.

    static void initAcquire( FileIoRequest *blockReq, void *fileHandle, size_t pos )
    {
        next_(blockReq) = 0;
        blockReq->resultStatus = 0;
        bytesCopied_(blockReq) = 0;
        blockReq->requestType = FileIoRequest::READ_BLOCK;

        blockReq->readBlock.fileHandle = fileHandle;
        blockReq->readBlock.filePosition = pos;
        blockReq->readBlock.dataBlock = 0;
        blockReq->readBlock.isAtEof = false;
        blockReq->readBlock.completionFlag.init();
    }

    static bool test_for_completion_async( FileIoRequest *blockReq )
    {
        return blockReq->readBlock.completionFlag.test_for_completion_async();
    }

    static bool try_cancel_async( FileIoRequest *blockReq )
    {
        return blockReq->readBlock.completionFlag.try_cancel_async();
    }

    static void receiveAcquire( FileIoRequest *r )
    {
        if (r->resultStatus==NOERROR) {

            void *fileHandle = r->readBlock.fileHandle;
            size_t filePosition = r->readBlock.filePosition;
            DataBlock *dataBlock = r->readBlock.dataBlock;

            state_(r) = BLOCK_STATE_READY;
            assert(r->requestType==FileIoRequest::RELEASE_READ_BLOCK);

            r->releaseReadBlock.fileHandle = fileHandle;
            r->releaseReadBlock.filePosition = filePosition;
            r->releaseReadBlock.dataBlock = dataBlock;
        
        } else {
            assert(r->readBlock.fileHandle==IO_INVALID_FILE_HANDLE);
            assert(r->readBlock.dataBlock==0);

            // Mark the request as in ERROR.
            // The stream state will switch to ERROR when the client tries to read/write the block.
            state_(r) = BLOCK_STATE_ERROR;
        }
    }

    // copyBlockData: Copy data into or out of the block. (out-of: read, in-to: write)

    typedef void* user_items_ptr_t; // this is const for write streams

    enum CopyStatus { CAN_CONTINUE, AT_BLOCK_END, AT_FINAL_BLOCK_END };
    /*
        NOTE if we wanted to support items that span multiple blocks we could
        introduce an extra status: NEED_NEXT_BLOCK that we would return if the
        next block is pending but we needed data from it to copy an item.
        NEED_NEXT_BLOCK would cause the wrapper to try to retire the block
        or to go into the buffering state.
    */

    static CopyStatus copyBlockData( FileIoRequest *blockReq, user_items_ptr_t userItemsPtr, 
            size_t maxItemsToCopy, size_t itemSizeBytes, size_t *itemsCopiedResult )
    {
        size_t blockBytesCopiedSoFar = bytesCopied_(blockReq);
        size_t bytesRemainingInBlock = dataBlock(blockReq)->validCountBytes - blockBytesCopiedSoFar;
        size_t wholeItemsRemainingInBlock = bytesRemainingInBlock / itemSizeBytes;

        // Assume that itemSize divides block size, otherwise we need to deal with items overlapping blocks 
        // and that wouldn't be time-efficient (see NOTE earlier for possible implementation).
        assert( wholeItemsRemainingInBlock*itemSizeBytes == bytesRemainingInBlock ); 

        size_t itemsToCopy = std::min<size_t>(wholeItemsRemainingInBlock, maxItemsToCopy);
        
        size_t bytesToCopy = itemsToCopy*itemSizeBytes;
        std::memcpy(userItemsPtr, static_cast<int8_t*>(dataBlock(blockReq)->data)+blockBytesCopiedSoFar, bytesToCopy);
        bytesCopied_(blockReq) += bytesToCopy;

        *itemsCopiedResult = itemsToCopy;

        if (itemsToCopy==wholeItemsRemainingInBlock) {
            if (blockReq->readBlock.isAtEof)
                return AT_FINAL_BLOCK_END;
            else
               return AT_BLOCK_END;
        } else {
            return CAN_CONTINUE;
        }
    }
};


struct WriteBlockRequestBehavior : public BlockRequestBehavior {

    // when not in-flight, the request type field represents the state of the block request.
    // the acquire request type is mapped to the PENDING state.
    enum BlockState {
        BLOCK_STATE_PENDING = FileIoRequest::ALLOCATE_WRITE_BLOCK,
        BLOCK_STATE_READY = FileIoRequest::RELEASE_UNMODIFIED_WRITE_BLOCK,
        BLOCK_STATE_READY_MODIFIED = FileIoRequest::COMMIT_MODIFIED_WRITE_BLOCK,
        BLOCK_STATE_ERROR = FileIoRequest::NO_OP,
    };

    // fields and predicates

    static DataBlock *dataBlock(FileIoRequest *r) { return r->commitOrReleaseWriteBlock.dataBlock; }
    static size_t filePosition(FileIoRequest *r) { return r->commitOrReleaseWriteBlock.filePosition; }

    static bool isReady(FileIoRequest *r) { return ((state_(r) == BLOCK_STATE_READY) || (state_(r) == BLOCK_STATE_READY_MODIFIED)); }

    // request initialization and transformation

    static void initAcquire( FileIoRequest *blockReq, void *fileHandle, size_t pos )
    {
        next_(blockReq) = 0;
        blockReq->resultStatus = 0;
        bytesCopied_(blockReq) = 0;

        blockReq->requestType = FileIoRequest::ALLOCATE_WRITE_BLOCK;
        blockReq->allocateWriteBlock.fileHandle = fileHandle;
        blockReq->allocateWriteBlock.filePosition = pos;
        blockReq->allocateWriteBlock.dataBlock = 0;
        blockReq->allocateWriteBlock.completionFlag.init();
    }

    static bool test_for_completion_async( FileIoRequest *blockReq )
    {
        return blockReq->allocateWriteBlock.completionFlag.test_for_completion_async();
    }

    static bool try_cancel_async( FileIoRequest *blockReq )
    {
        return blockReq->allocateWriteBlock.completionFlag.try_cancel_async();
    }

    static void receiveAcquire( FileIoRequest *r )
    {
        if (r->resultStatus==NOERROR) {

            void *fileHandle = r->allocateWriteBlock.fileHandle;
            size_t filePosition = r->allocateWriteBlock.filePosition;
            DataBlock *dataBlock = r->allocateWriteBlock.dataBlock;

            state_(r) = BLOCK_STATE_READY;
            assert(r->requestType==FileIoRequest::RELEASE_UNMODIFIED_WRITE_BLOCK);

            r->commitOrReleaseWriteBlock.fileHandle = fileHandle;
            r->commitOrReleaseWriteBlock.filePosition = filePosition;
            r->commitOrReleaseWriteBlock.dataBlock = dataBlock;

        } else {
            assert(r->allocateWriteBlock.fileHandle==IO_INVALID_FILE_HANDLE);
            assert(r->allocateWriteBlock.dataBlock==0);

            // Mark the request as in ERROR.
            // The stream state will switch to ERROR when the client tries to read/write the block.
            state_(r) = BLOCK_STATE_ERROR;
        }
    }

    // copyBlockData: Copy data into or out of the block. (out-of: read, in-to: write)

    typedef const void* user_items_ptr_t;

    enum CopyStatus { CAN_CONTINUE, AT_BLOCK_END, AT_FINAL_BLOCK_END };

    static CopyStatus copyBlockData( FileIoRequest *blockReq, user_items_ptr_t userItemsPtr, 
        size_t maxItemsToCopy, size_t itemSizeBytes, size_t *itemsCopiedResult )
    {
        size_t blockBytesCopiedSoFar = bytesCopied_(blockReq);
        size_t bytesRemainingInBlock = IO_DATA_BLOCK_DATA_CAPACITY_BYTES - blockBytesCopiedSoFar;
        size_t wholeItemsRemainingInBlock = bytesRemainingInBlock / itemSizeBytes;

        // Assume that itemSize divides block size, otherwise we need to deal with items overlapping blocks 
        // and that wouldn't be time-efficient (see NOTE earlier for possible implementation).
        assert( wholeItemsRemainingInBlock*itemSizeBytes == bytesRemainingInBlock ); 

        if (dataBlock(blockReq)->validCountBytes < blockBytesCopiedSoFar) {
            // if bytesCopied_ is manipulated to start writing somewhere other than the start of the block
            // we may have a situation where we need to zero the first part of the block.

            std::memset(static_cast<int8_t*>(dataBlock(blockReq)->data)+dataBlock(blockReq)->validCountBytes, 
                    0, blockBytesCopiedSoFar-dataBlock(blockReq)->validCountBytes);
            dataBlock(blockReq)->validCountBytes = blockBytesCopiedSoFar;
        }

        size_t itemsToCopy = std::min<size_t>(wholeItemsRemainingInBlock, maxItemsToCopy);

        size_t bytesToCopy = itemsToCopy*itemSizeBytes;
        std::memcpy(static_cast<int8_t*>(dataBlock(blockReq)->data)+blockBytesCopiedSoFar, userItemsPtr, bytesToCopy);
        bytesCopied_(blockReq) += bytesToCopy;

        dataBlock(blockReq)->validCountBytes = bytesCopied_(blockReq);
        state_(blockReq) = BLOCK_STATE_READY_MODIFIED;
        assert(r->requestType==FileIoRequest::COMMIT_MODIFIED_WRITE_BLOCK);

        *itemsCopiedResult = itemsToCopy;

        if (itemsToCopy==wholeItemsRemainingInBlock) {
            return AT_BLOCK_END;
        } else {
            return CAN_CONTINUE;
        }
    }
};


template< typename BlockReq, typename StreamType >
class FileIoStreamWrapper { // Object-oriented wrapper for a read and write streams

    typedef StreamType STREAMTYPE; // READSTREAM or WRITESTREAM

    FileIoRequest *openFileReq_; // The data structure is represented by a linked structure of FileIoRequest objects

    /*
        The stream data structure is composed of linked request nodes.
        The prefetch queue is built using the links_ fields of openFileReq_.
        This works because these fields are not otherwise used after openFileReq_ has completed.

             READSTREAM* (or WRITESTREAM*)
                 |
                 | (openFileReq_)
                 V 
           [ OPEN_FILE ] ------------------------------------
                 |   (openFileReq)                          |
          (head) |                                   (tail) | 
                 V                                          V
          [ READ_BLOCK ] -> [ READ_BLOCK ] -> ... -> [ READ_BLOCK ] -> NULL    } (prefetchQueue)


       "[ ... ]" indicates a FileIoRequest structure.

       For a write stream, the prefetch queue contains ALLOCATE_WRITE_BLOCK requests.
    */

    // Stream field lvalue aliases. Map/alias request fields to fields of our pseudo-class.

    FileIoRequest* openFileReq() { return openFileReq_; }
    int& state_() { return openFileReq_->requestType; }
    int& error_() { return openFileReq_->resultStatus; }
    FileIoRequest*& prefetchQueueHead_() { return openFileReq_->links_[FileIoRequest::TRANSIT_NEXT_LINK_INDEX]; }
    FileIoRequest*& prefetchQueueTail_() { return openFileReq_->links_[FileIoRequest::CLIENT_NEXT_LINK_INDEX]; }
    size_t& waitingForBlocksCount_() { return openFileReq_->clientInt; }

    FileIoStreamWrapper( FileIoRequest *openFileReq )
        : openFileReq_( openFileReq ) {}

    void receiveOpenFileReq()
    {
        FileIoRequest *r=openFileReq();

        r->openFile.path->release();
        r->openFile.path = 0;

        if (r->resultStatus==NOERROR) {
            assert( r->openFile.fileHandle != 0 );

            prefetchQueueHead_() = 0;
            prefetchQueueTail_() = 0;
            waitingForBlocksCount_() = 0;

            state_() = STREAM_STATE_OPEN_IDLE;
            // NOTE: In principle we could seek here. at the moment we require the client to poll for idle.
        } else {
            error_() = r->resultStatus;
            state_() = STREAM_STATE_ERROR;
        }

        // Leave openFileReq linked to the structure, even if there's an error.
    }

    void cleanupReceivedOpenFileReq()
    {
        FileIoRequest *r = openFileReq_;
        openFileReq_ = 0;

        assert (prefetchQueueHead_() == 0);
        assert (prefetchQueueTail_() == 0);
        assert (waitingForBlocksCount_() == 0);

        if (r->openFile.fileHandle != IO_INVALID_FILE_HANDLE) {
            // Transform openFileReq to CLOSE_FILE and send to server
            void *fileHandle = r->openFile.fileHandle;

            FileIoRequest *closeFileReq = r;
            closeFileReq->requestType = FileIoRequest::CLOSE_FILE;
            closeFileReq->closeFile.fileHandle = fileHandle;
            ::sendFileIoRequestToServer(closeFileReq);
        } else {
            freeFileIoRequest(r);
        }
    }


    FileIoRequest* prefetchQueue_front()
    {
        return prefetchQueueHead_();
    }

    void prefetchQueue_pop_front()
    {
        FileIoRequest *x = prefetchQueueHead_();
        prefetchQueueHead_() = BlockReq::next_(x);
        BlockReq::next_(x) = 0;
    }

    void prefetchQueue_push_back( FileIoRequest *blockReq )
    {
        assert( prefetchQueueTail_() != 0 ); // doesn't deal with an empty queue, doesn't need to
        BlockReq::next_(prefetchQueueTail_()) = blockReq;
        prefetchQueueTail_() = blockReq;
    }


    void sendAcquireBlockRequestToServer( FileIoRequest *blockReq )
    {
        ::sendFileIoRequestToServer(blockReq);
        ++waitingForBlocksCount_();
    }

    void sendAcquireBlockRequestsToServer( FileIoRequest *front, FileIoRequest *back, size_t count )
    {
        ::sendFileIoRequestsToServer(front, back);
        waitingForBlocksCount_() += count;
    }

    // Init, link and send sequential block request
    void initAndLinkSequentialAcquireBlockRequest( FileIoRequest *blockReq )
    {
        // Init, link, and send a sequential data block acquire request (READ_BLOCK or ALLOCATE_WRITE_BLOCK).
        // Init the block request so that it's file position directly follows the tail block in the prefetch queue;
        // link the request onto the back of the prefetch queue; send the request to the server.

        // precondition: prefetch queue is non-empty
        assert( prefetchQueueHead_() !=0 && prefetchQueueTail_() !=0 );

        BlockReq::initAcquire( blockReq, openFileReq()->openFile.fileHandle,
                BlockReq::filePosition(prefetchQueueTail_()) + IO_DATA_BLOCK_DATA_CAPACITY_BYTES );

        prefetchQueue_push_back(blockReq);
    }

    void receiveAcquireBlockReq( FileIoRequest *r )
    {
        if (--waitingForBlocksCount_() == 0)
            state_() = STREAM_STATE_OPEN_STREAMING;

        BlockReq::receiveAcquire(r); // this will transition to READY or ERROR. In READY the block fields need to be avialable
    }

    template< typename ReturnToServerFunc >
    void flushBlock( FileIoRequest *blockReq, ReturnToServerFunc returnToServer )
    {
        if (BlockReq::state_(blockReq) == BlockReq::BLOCK_STATE_PENDING) {

            if (BlockReq::try_cancel_async(blockReq)) {
                --waitingForBlocksCount_();
                return; // block has been canceled we're done
            } else {
                // block was completed in the process of trying to cancel
                receiveAcquireBlockReq(blockReq);
            
                /* FALLS THROUGH */
            }
        }

        switch (BlockReq::state_(blockReq))
        {
        case BlockReq::BLOCK_STATE_PENDING:
            assert( false ); // pending case was handled above
            break;

        case BlockReq::BLOCK_STATE_READY:
            BlockReq::next_(blockReq) = 0;
            returnToServer(blockReq);
            break;

        case BlockReq::BLOCK_STATE_READY_MODIFIED:
            BlockReq::next_(blockReq) = 0;
            returnToServer(blockReq);
            break;

        case BlockReq::BLOCK_STATE_ERROR:
            freeFileIoRequest( blockReq );
            break;
        }
    }

    void flushPrefetchQueue()
    {
        // send all blocks to the server for cleanup in whatever state they are in

        FileIoRequest *blockReq = prefetchQueueHead_();
        prefetchQueueHead_() = 0;
        prefetchQueueTail_() = 0;

        if (blockReq) {
            blockReq->requestType |= FileIoRequest::FLUSH_PREFETCH_QUEUE_FLAG;
            ::sendFileIoRequestToServer(blockReq);
        }
    }

    // Should only be called after the stream has been opened and before it is closed.
    // returns true if a block was processed.
    bool receiveOneBlock()
    {
        // scan through the list until a block has been retired or the end of the list is reached

        // TODO: ideally we'd cache our queue index here so that we don't have to traverse every time

        for (FileIoRequest *r = prefetchQueueHead_(); r != 0; r = BlockReq::next_(r)) {
            if (BlockReq::state_(r) == BlockReq::BLOCK_STATE_PENDING) {
                if (BlockReq::test_for_completion_async(r)) {
                    receiveAcquireBlockReq(r);
                    return true;
                }
            }
        }

        return false;
    }

    void receiveAllPendingBlocks()
    {
        for (FileIoRequest *r = prefetchQueueHead_(); (r != 0) && (waitingForBlocksCount_() > 0); r = BlockReq::next_(r)) {
            if (BlockReq::state_(r) == BlockReq::BLOCK_STATE_PENDING) {
                if (BlockReq::test_for_completion_async(r))
                    receiveAcquireBlockReq(r);
            }
        }
    }
    
    bool advanceToNextBlock()
    {
        // issue next block request, link it on to the tail of the prefetch queue...
        
        FileIoRequest *newBlockReq = allocFileIoRequest();
        if (!newBlockReq) {
            // Fail. couldn't allocate request
            state_() = STREAM_STATE_ERROR;
            return false;
        }
        initAndLinkSequentialAcquireBlockRequest(newBlockReq);
        sendAcquireBlockRequestToServer(newBlockReq);

        // unlink and flush the old block...

        // Notice that we link the new request on the back of the prefetch queue before unlinking
        // the old one off the front, so there is no chance of having to deal with the special case of
        // linking to an empty queue.

        FileIoRequest *oldBlockReq = prefetchQueue_front();
        prefetchQueue_pop_front(); // advance head to next block
        flushBlock(oldBlockReq, ::sendFileIoRequestToServer);
        
        return true;
    }

    static size_t roundDownToBlockSizeAlignedPosition( size_t pos )
    {
        size_t blockNumber = pos / IO_DATA_BLOCK_DATA_CAPACITY_BYTES;
        return blockNumber * IO_DATA_BLOCK_DATA_CAPACITY_BYTES;
    }

public:
    FileIoStreamWrapper( STREAMTYPE *fp )
        : openFileReq_( static_cast<FileIoRequest*>(fp) ) {}
    
    static STREAMTYPE* open( SharedBuffer *path, FileIoRequest::OpenMode openMode )
    {
        // Return 0 if allocation fails.

        FileIoRequest *openFileReq = allocFileIoRequest();
        if (!openFileReq)
            return 0;
        
        // Initialise the stream data structure

        FileIoStreamWrapper stream(openFileReq);

        // Note: some of these alias openFileReq request fields.
        stream.state_() = STREAM_STATE_OPENING;
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
        openFileReq->openFile.completionFlag.init();

        ::sendFileIoRequestToServer(openFileReq);

        return static_cast<STREAMTYPE*>(openFileReq);
    }

    void close()
    {
        // (Don't poll state, just dispose current state)

        if (state_()==STREAM_STATE_OPENING) {

            // Still waiting for OPEN_FILE to return.
            // Attempt to cancel the open request. but if if completes, process it
            if (!openFileReq()->openFile.completionFlag.try_cancel_async()) {
                receiveOpenFileReq();
                cleanupReceivedOpenFileReq();
            }
            
        } else {
            // Stream is open. The prefetch queue may contain requests.

            // Dispose the prefetch queue, if it's populated

            flushPrefetchQueue();

            // Clean up the open file request
            cleanupReceivedOpenFileReq();
        }
    }

    int seek( size_t pos )
    {
        assert( pos >= 0 );

        if (state_() == STREAM_STATE_OPENING || state_() == STREAM_STATE_ERROR)
            return -1;

        // Straight-forward implementation of seek: dump all blocks from the prefetch queue, 
        // then request the needed blocks.
        // A more optimised version would retain any needed blocks from the current prefetch queue.

        flushPrefetchQueue();

        // HACK: Hardcode the prefetch queue length.
        // The prefetch queue length should be computed from the stream data rate and the 
        // desired prefetch buffering length (in seconds).
        
        const int prefetchQueueBlockCount = 20;

        // Request blocks on block-size-aligned boundaries
        size_t blockFilePositionBytes = roundDownToBlockSizeAlignedPosition(pos);

        // request the first block 

        FileIoRequest *firstBlockReq = allocFileIoRequest();
        if (!firstBlockReq) {
            state_() = STREAM_STATE_ERROR;
            return -1;
        }

        BlockReq::initAcquire( firstBlockReq, openFileReq()->openFile.fileHandle, blockFilePositionBytes );

        BlockReq::bytesCopied_(firstBlockReq) = pos - blockFilePositionBytes; // compensate for block-size-aligned request
        
        prefetchQueueHead_() = firstBlockReq;
        prefetchQueueTail_() = firstBlockReq;

        // Optimisation: enqueue all block requests at once. First link them into a list, then enqueue them.
        // This minimises contention on the communication queue by only invoking a single push operation.

        // Construct a linked list of block requests to enqueue, firstBlockReq will be at the tail.
        // lastBlockReq -> ... -> firstBlockReq -> 0
        QwSList<FileIoRequest*, FileIoRequest::TRANSIT_NEXT_LINK_INDEX> blockRequests;
        BlockReq::transitNext_(firstBlockReq) = 0;
        blockRequests.push_front(firstBlockReq);
    
        for (int i=1; i < prefetchQueueBlockCount; ++i) {
            FileIoRequest *blockReq = allocFileIoRequest();
            if (!blockReq) {
                // Fail. couldn't allocate request.

                // Rollback: deallocate all allocated block requests
                while (!blockRequests.empty()) {
                    FileIoRequest *r = blockRequests.front();
                    blockRequests.pop_front();
                    freeFileIoRequest(r);
                }

                state_() = STREAM_STATE_ERROR;
                return -1;
            }

            initAndLinkSequentialAcquireBlockRequest(blockReq);
            blockRequests.push_front(blockReq);
        }

        sendAcquireBlockRequestsToServer(blockRequests.front(), firstBlockReq, prefetchQueueBlockCount);
        
        state_() = STREAM_STATE_OPEN_BUFFERING;

        return 0;
    }

    typedef typename BlockReq::user_items_ptr_t user_items_ptr_t;

    size_t read_or_write( user_items_ptr_t userItemsPtr, size_t itemSizeBytes, size_t itemCount ) // for a read stream this is read(), for a write stream this is write()
    {
        // Always process at least one expected reply from the server per read/write call by calling pollState().
        // If read_or_write() reads at most N bytes, and (IO_DATA_BLOCK_DATA_CAPACITY_BYTES/N)
        // is greater than the maximum result queue length, then this single poll is almost always
        // sufficient to retire all pending results before then are needed. The conditions where it
        // isn't sufficient require that the prefetch buffer must be slightly longer than if we
        // processed all pending replies as soon as they were available (consider the case where
        // replies arrive in reverse order just before their deadline).

        pollState(); // Updates state based on received replies. e.g. from BUFFERING to STREAMING

        switch (state_())
        {
        case STREAM_STATE_OPENING:
        case STREAM_STATE_OPEN_IDLE:
        case STREAM_STATE_OPEN_EOF:
        case STREAM_STATE_ERROR:
            return 0;
            break;

        case STREAM_STATE_OPEN_BUFFERING:
            // In BUFFERING the stream can still read or write if there is
            // data available. Hence the fall-through below. If clients want to
            // pause while the stream is buffering they need to poll the state and
            // implement their own pause logic.
            {
#if defined(IO_USE_CONSTANT_TIME_RESULT_POLLING)
                // fall through
#else
                // The call to pollState() above only deals with at most one pending block.
                // To reduce the latency of transitioning from  BUFFERING to STREAMING we can drain the result queue here.
                // This is O(N) in the number of expected results.
                receiveAllPendingBlocks();
#endif         
            }
            /* FALLS THROUGH */

        case STREAM_STATE_OPEN_STREAMING:
            {
                int8_t *userBytesPtr = (int8_t*)userItemsPtr;
                const size_t maxItemsToCopy = itemCount;
                size_t itemsCopiedSoFar = 0;

                while (itemsCopiedSoFar < maxItemsToCopy) {
                    FileIoRequest *frontBlockReq = prefetchQueue_front();
                    assert( frontBlockReq != 0 );

                    // Last-ditch effort to determine whether the front block has been returned.  O(1)
                    // Usually the call to pollState() above will receive the frontmost
                    // block in the prefetch queue.
                    // However if this loop is copying a large amount of data we may need
                    // to receive additional blocks due to calls to advanceToNextBlock() below.
                    if (BlockReq::state_(frontBlockReq) == BlockReq::BLOCK_STATE_PENDING) {
                        if (BlockReq::test_for_completion_async(frontBlockReq))
                            receiveAcquireBlockReq(frontBlockReq);
                    }
                    
                    if (BlockReq::isReady(frontBlockReq)) {
                        // copy data to/from userItemsPtr and the front block in the prefetch queue

                        size_t itemsRemainingToCopy = maxItemsToCopy - itemsCopiedSoFar;

                        size_t itemsCopied = 0;
                        typename BlockReq::CopyStatus copyStatus = BlockReq::copyBlockData(frontBlockReq, userBytesPtr, itemsRemainingToCopy, itemSizeBytes, &itemsCopied);

                        userBytesPtr += itemsCopied * itemSizeBytes;
                        itemsCopiedSoFar += itemsCopied;

                        switch (copyStatus) {
                        case BlockReq::AT_BLOCK_END:
                            if (!advanceToNextBlock())
                                return itemsCopiedSoFar; // advance failed
                            break;
                        case BlockReq::AT_FINAL_BLOCK_END:
                            state_() = STREAM_STATE_OPEN_EOF;
                            return itemsCopiedSoFar;
                            break;
                        case BlockReq::CAN_CONTINUE:
                            /* NOTHING */
                            break;
                        }
                    } else if(BlockReq::state_(frontBlockReq) == BlockReq::BLOCK_STATE_PENDING) {
                        state_() = STREAM_STATE_OPEN_BUFFERING;
                        return itemsCopiedSoFar;
                    } else {
                        assert( BlockReq::state_(frontBlockReq) == BlockReq::BLOCK_STATE_ERROR );
                        state_() = STREAM_STATE_ERROR;
                        return itemsCopiedSoFar;
                    }
                }

                assert( itemsCopiedSoFar == maxItemsToCopy );
                return maxItemsToCopy;
            }
            break;
        }

        return 0;
    }

    FileIoStreamState pollState()
    {
        if (state_()==STREAM_STATE_OPENING) {

            if (openFileReq()->openFile.completionFlag.test_for_completion_async())
                receiveOpenFileReq();

        } else {
            if (waitingForBlocksCount_() > 0) {

                assert( state_()==STREAM_STATE_OPEN_IDLE 
                    || state_()==STREAM_STATE_OPEN_EOF
                    || state_()==STREAM_STATE_OPEN_BUFFERING 
                    || state_()==STREAM_STATE_OPEN_STREAMING
                    || state_()==STREAM_STATE_ERROR );

                receiveOneBlock();
            }
        }

        return (FileIoStreamState)state_();
    }

    int getError()
    {
        return error_();
    }
};


// read stream

typedef FileIoStreamWrapper<ReadBlockRequestBehavior,READSTREAM> FileIoReadStreamWrapper;

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
    return FileIoReadStreamWrapper(fp).read_or_write(dest, itemSize, itemCount);
}

FileIoStreamState FileIoReadStream_pollState( READSTREAM *fp )
{
    return FileIoReadStreamWrapper(fp).pollState();
}

int FileIoReadStream_getError( READSTREAM *fp )
{
    return FileIoReadStreamWrapper(fp).getError();
}


// write stream

typedef FileIoStreamWrapper<WriteBlockRequestBehavior,WRITESTREAM> FileIoWriteStreamWrapper;

WRITESTREAM *FileIoWriteStream_open( SharedBuffer *path, FileIoRequest::OpenMode openMode )
{
    return FileIoWriteStreamWrapper::open(path, openMode);
}

void FileIoWriteStream_close( WRITESTREAM *fp )
{
    FileIoWriteStreamWrapper(fp).close();
}

int FileIoWriteStream_seek( WRITESTREAM *fp, size_t pos )
{
    return FileIoWriteStreamWrapper(fp).seek(pos);
}

size_t FileIoWriteStream_write( const void *src, size_t itemSize, size_t itemCount, WRITESTREAM *fp )
{
    return FileIoWriteStreamWrapper(fp).read_or_write(src, itemSize, itemCount);
}

FileIoStreamState FileIoWriteStream_pollState( WRITESTREAM *fp )
{
    return FileIoWriteStreamWrapper(fp).pollState();
}

int FileIoWriteStream_getError( WRITESTREAM *fp )
{
    return FileIoWriteStreamWrapper(fp).getError();
}

/*
o- could possibly replace the waiting count with a pointer that runs through the 
prefetch queue polling requests for completion. when the pointer is 0 all requests have completed (maybe)
could store waiting count in resultCode field and use the user pointer to store the receiveBlockReq ptr

*/