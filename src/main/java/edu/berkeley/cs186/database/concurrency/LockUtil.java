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
        // S: To get S lock on a node, must hold IS on parent node
        // X: To get X on a node, must hold IX on parent node
        // NL: Requesting NL should do nothing
        if (requestType == LockType.S) {
            ensureAncestors(lockContext, LockType.IS);
        } else if (requestType == LockType.X) {
            ensureAncestors(lockContext, LockType.IX);
        } else {
            // requestType == LockType.NL
            return;
        }

        if (LockType.substitutable(effectiveLockType, requestType)) {
            // case 1: current lock type can effectively substitute the requested type
            return;
        } else if (explicitLockType == LockType.IX && requestType == LockType.S) {
            // case 2: current lock type is IX and the requested lock is S
            lockContext.promote(transaction, LockType.SIX);
        } else if (explicitLockType.isIntent()) {
            // case 3: current lock type is an intent lock
            lockContext.escalate(transaction);
            ensureSufficientLockHeld(lockContext, requestType);
        } else if (explicitLockType == LockType.NL) {
            // case 4 pt 1: none of the above (explicitLockType == NL)
            lockContext.acquire(transaction, requestType);
        } else {
            // case 4 pt 2: none of the above (explicitLockType == S or X)
            lockContext.promote(transaction, requestType);
        }
    }

    // TODO(proj4_part2) add any helper methods you want
    // ensures you have the appropriate locks on all ancestors
    // appropriate will either be IS or IX
    public static void ensureAncestors(LockContext lockContext, LockType appropriate) {
        TransactionContext transaction = TransactionContext.getTransaction();

        // iterate through all ancestors, starting from root
        for (LockContext pContext : getAncestors(lockContext)) {
            LockType pType = pContext.getEffectiveLockType(transaction);
            if (pType != appropriate) {
                if (pType == LockType.NL) {
                    pContext.acquire(transaction, appropriate);
                } else if (pType == LockType.S && appropriate == LockType.IX) {
                    pContext.promote(transaction, LockType.SIX);
                } else if (LockType.substitutable(appropriate, pType)) {
                    pContext.promote(transaction, appropriate);
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
