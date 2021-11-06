package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        // check ancestors
        // *requestType must be S, X, NL
        // To get S lock on a node, must hold ISon parent node
        // To get X on a node, must hold IX on parent node
        if (requestType == LockType.S) {
            ensureAncestors(lockContext, LockType.IS);
        } else if (requestType == LockType.X) {
            ensureAncestors(lockContext, LockType.IX);
        }

        // case 1: current lock type can effectively substitute the requested type
        if (LockType.substitutable(effectiveLockType, requestType)) {
            lockContext.promote(transaction, requestType);
        }
        // case 2: current lock type is IX and the requested lock is S
        else if (explicitLockType == LockType.IX && requestType == LockType.S) {
            lockContext.promote(transaction, LockType.SIX);
        }
        // case 3: current lock type is an intent lock
        else if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction);
        }
        // case 4: none of the above
        // if LockType is NL
        else if (explicitLockType == LockType.NL) {
            lockContext.acquire(transaction, requestType);
        }
        else {
            // blahbalbhab
            return;
        }
    }

    // TODO(proj4_part2) add any helper methods you want
    // ensures you have the appropriate locks on all ancestors
    public static void ensureAncestors(LockContext lockContext, LockType reqType) {
        TransactionContext transaction = TransactionContext.getTransaction();
        ArrayDeque<LockContext> ancestors = getAncestors(lockContext);

        // iterate through all ancestors, starting from root
        for (LockContext pContext : ancestors) {
            LockType pType = pContext.getEffectiveLockType(transaction);
            if (pType != reqType) {
                if (LockType.substitutable(pType, reqType)) {
                    pContext.promote(transaction, reqType);
                } else if ((pType == LockType.IX && reqType == LockType.S) || (pType == LockType.S && reqType == LockType.IX)) {
                    pContext.promote(transaction, LockType.SIX);
                } else if (reqType == LockType.NL) {
                    lockContext.acquire(transaction, reqType);
                }
            }
        }
    }

    // helper method to get all ancestors
    public static ArrayDeque<LockContext> getAncestors(LockContext lockContext) {
        LockContext parContext = lockContext.parentContext();
        ArrayDeque<LockContext> ancestors = new ArrayDeque<LockContext>();
        while (parContext != null) {
            // add to front of arrayDeque so goes top->bottom
            ancestors.addFirst(parContext);
            parContext = parContext.parentContext();
        }
        return ancestors;
    }
}
