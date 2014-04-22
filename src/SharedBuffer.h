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
#ifndef INCLUDED_SHAREDBUFFER_H
#define INCLUDED_SHAREDBUFFER_H

#include <cstddef> // max_align_t

#include "mintomic/mintomic.h"

/*
    Reference counted shared buffer with real-time safe deallocation.

    Used for path strings.
*/

struct SharedBuffer{
    union{
        mint_atomic32_t refCount;
        //std::max_align_t align_;
        long double align_;
    };
    
    char data[1];

    // the following calls are real-time safe
    void addRef()
    {
        mint_fetch_add_32_relaxed(&refCount,1);
    }

    void release();
};

// The following assumes only a single shared buffer allocation domain. 
// We may have multiple domains, in which case may be better to abstract 
// over an abstract allocator.


// None of these methods are real-time safe
struct SharedBufferAllocator {
    
    static SharedBuffer* alloc( const char *s );

    // Call this once at the end of the program to ensure that all paths are freed.
    // This is called internally whenever you allocate a new shared buffer, 
    // so you don't need to call it during program execution.
    static void reclaimMemory();

    static void checkForLeaks();

private:
    friend struct SharedBuffer;
    static void enqueueForReclamation( SharedBuffer *p ); // INTERNAL USE
};


inline void SharedBuffer::release()
{
    if (mint_fetch_add_32_relaxed(&refCount,-1)==1)
        SharedBufferAllocator::enqueueForReclamation(this);
}

#endif /* INCLUDED_SHAREDBUFFER_H */
