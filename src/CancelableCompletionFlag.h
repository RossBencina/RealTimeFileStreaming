#ifndef INCLUDED_CANCELABLECOMPLETIONFLAG_H
#define INCLUDED_CANCELABLECOMPLETIONFLAG_H

#include <cassert>

#include "mintomic/mintomic.h"

struct CancelableCompletionFlag {

    enum { INITIAL, COMPLETED, CANCELED };

    mint_atomic32_t flag_;

    // Client / consumer methods: init, cancel, test_for_completion

    // init
    // Place the flag in the initial state. This should be done
    // before passing the flag to the server.
    void init()
    {
        flag_._nonatomic = INITIAL;
    }

    // cancel
    // Attempt to cancel a pending operation
    // i.e. transition from INITIAL --> CANCELED
    // Returns true if the flag ends in the CANCELED state (idempotent to multiple cancellations).
    // If the operation has completed, returns false
    // and performs an /acquire/ fence so that the operation's result data can be used.

    bool try_cancel_async()
    {
        if (test_for_completion_async()) {
            return false; // cancel failed
        } else {
            uint32_t oldState = mint_compare_exchange_strong_32_relaxed(&flag_, INITIAL, CANCELED);
            if (oldState==INITIAL) {
                return true;
            } else if(oldState==COMPLETED) {
                mint_thread_fence_acquire();
                return false;
            } else {
                // shouldn't really be calling cancel() multiple times
                //assert( false );
                assert(oldState==CANCELED);
                return true;
            }
        }
    }

    bool try_cancel_sync()
    {
        if (flag_._nonatomic==INITIAL) {
            flag_._nonatomic=CANCELED;
            return true;
        } else if (flag_._nonatomic==COMPLETED) {
            return false;
        } else {
            assert(flag_._nonatomic==CANCELED);
            return true;
        }
    }

    // test_for_completion
    // Test for completion
    // If the operation has completed, return true
    // and perform an /acquire/ fence so that the operation's result data can be used.
    // Returns false if the operation is pending or canceled.
    // NOTE: The consumer shouldn't really be testing a canceled flag.

    bool test_for_completion_async()
    {
        if (mint_load_32_relaxed(&flag_)==COMPLETED) {
            mint_thread_fence_acquire();
            return true;
        } else {
            return false;
        }
    }

    bool test_for_completion_sync()
    {
        if (flag_._nonatomic==COMPLETED) {
            return true;
        } else {
            return false;
        }
    }

    // Server / producer methods: complete, has_been_canceled

    // complete
    // Signal that the operation has completed.
    // i.e. transition from INITIAL --> COMPLETED
    // Returns true if the flag ends in the COMPLETED state (idempotent to multiple completions).
    // Performs a /release/ fence so that the operation's result data can be consumed.
    // If the operation was previously canceled return false.
    bool try_complete_async()
    {
        if (has_been_canceled_async()) {
            return false;
        } else {
            mint_thread_fence_release();
            uint32_t oldState = mint_compare_exchange_strong_32_relaxed(&flag_, INITIAL, COMPLETED);
            if (oldState==INITIAL) {
                return true;
            } else if(oldState==CANCELED) {
                return false;
            } else {
                // shouldn't really be calling complete() multiple times
                //assert( false );
                assert(oldState==COMPLETED);
                return true;
            }
        }
    }

    // has_been_canceled
    // Poll for whether the flag has been signaled as canceled.
    bool has_been_canceled_async()
    {
        return (mint_load_32_relaxed(&flag_) == CANCELED);
    }
};

#endif /* INCLUDED_CANCELABLECOMPLETIONFLAG_H */
