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
#include "SharedBuffer.h"

#include <cassert>
#include <cstring> // strcpy
#include <cstdlib>

#include "QwMPMCPopAllLifoStack.h"

#ifndef NDEBUG
#define DEBUG_COUNT_SHARED_BUFFER_ALLOCATIONS
#endif

namespace {
  
struct MemoryBlock {
    MemoryBlock *links_[1];
};

static QwMPMCPopAllLifoStack<MemoryBlock*,0> reclaimQueue_;

#ifdef DEBUG_COUNT_SHARED_BUFFER_ALLOCATIONS
mint_atomic32_t allocCount_ = {0};
#endif

} // end anonymous namespace


void SharedBufferAllocator::enqueueForReclamation( SharedBuffer *p )
{
    MemoryBlock *b = reinterpret_cast<MemoryBlock*>(p);
    b->links_[0] = 0;
    reclaimQueue_.push(b);
}

void SharedBufferAllocator::reclaimMemory()
{
    MemoryBlock *b = reclaimQueue_.pop_all();

    while (b){
        MemoryBlock *k = b;
        b = k->links_[0];

        std::free(k);

#ifdef DEBUG_COUNT_SHARED_BUFFER_ALLOCATIONS
        mint_fetch_add_32_relaxed(&allocCount_,-1);
#endif
    }
}

SharedBuffer* SharedBufferAllocator::alloc( const char *s )
{
    reclaimMemory();

    SharedBuffer *result = (SharedBuffer*)std::malloc( sizeof(SharedBuffer) + strlen(s) );
    if (result) {
        result->refCount._nonatomic = 1;
        std::strcpy(result->data, s);

#ifdef DEBUG_COUNT_SHARED_BUFFER_ALLOCATIONS
        mint_fetch_add_32_relaxed(&allocCount_,1);
#endif
    }    
    
    return result;
}

void SharedBufferAllocator::checkForLeaks()
{
#ifdef DEBUG_COUNT_SHARED_BUFFER_ALLOCATIONS
    assert( allocCount_._nonatomic == 0 );
#endif
}