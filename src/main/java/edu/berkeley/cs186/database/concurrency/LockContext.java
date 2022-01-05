package edu.berkeley.cs186.database.concurrency;


import edu.berkeley.cs186.database.DatabaseException;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement

        if (this.readonly) {
            throw new UnsupportedOperationException("Error: This Lock Context is readonly!");
        }
        if (lockman.getLockType(transaction, name) != LockType.NL) {
            throw new DuplicateLockRequestException("Error: Duplicate Lock!");
        }
        if (parentContext() != null && !LockType.canBeParentLock(
                parentContext().getExplicitLockType(transaction), lockType)) {
            throw new InvalidLockException("Error: Invalid Lock Acquire!");
        }
        // lockmanager acquire lock
        this.lockman.acquire(transaction, name, lockType);
        // update numChildLocks
        if (parentContext() != null) {
            parentContext().numChildLocks.putIfAbsent(transaction.getTransNum(), 0);
            parentContext().numChildLocks.put(transaction.getTransNum(), parentContext().getNumChildren(transaction) + 1);
        }
        // return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

        if (readonly) {
            throw new UnsupportedOperationException("Error: This Lock Context is readonly!");
        }
        if (lockman.getLockType(transaction, name) == LockType.NL) {
            throw new NoLockHeldException("Error: No Lock!");
        }
        if (numChildLocks.getOrDefault(transaction.getTransNum(), 0) > 0) {
            throw new InvalidLockException("Error: Invalid Lock Release!");
        }
        this.lockman.release(transaction, name);
        if (parentContext() != null) {
            if (parentContext().getNumChildren(transaction) < 1) {
                // no more children for transaction
                throw new DatabaseException("Error: No Children for given transaction!");
            }
            parentContext().numChildLocks.put(transaction.getTransNum(),
                    parentContext().getNumChildren(transaction) - 1);
        }
        // return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("Error: This Lock Context is readonly!");
        }
        LockType oldLT = getExplicitLockType(transaction);
        if (oldLT == LockType.NL) {
            throw new NoLockHeldException("Error: No Lock!");
        }
        if (oldLT == newLockType) {
            throw new DuplicateLockRequestException("Error: Duplicate Lock!");
        }
        if (!LockType.substitutable(newLockType, oldLT) &&
                !(hasSIXAncestor(transaction) && newLockType == LockType.SIX) &&
                !(newLockType == LockType.SIX &&
                        (oldLT == LockType.S || oldLT == LockType.IS || oldLT == LockType.IX))) {
            throw new InvalidLockException("Error: Invalid Lock!");
        }
        if (newLockType == LockType.SIX && (oldLT == LockType.IS || oldLT == LockType.IX)) {
            // get S / IS descendants to release
            List<ResourceName> releaseDesc = sisDescendants(transaction);
            releaseDesc.add(0, this.name);
            // release old locks, acquire new locks
            lockman.acquireAndRelease(transaction, name, newLockType, releaseDesc);
            // update numChildLocks
            for (ResourceName rName: releaseDesc.subList(1, releaseDesc.size())) {
                LockContext currLC = LockContext.fromResourceName(lockman, rName);
                LockContext parentLC = currLC.parentContext();
                parentLC.numChildLocks.put(transaction.getTransNum(),
                        parentLC.numChildLocks.get(transaction.getTransNum()) - 1);
            }
        } else {
            // no S / IS locks to release
            lockman.promote(transaction, name, newLockType);
        }
        // return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("Error: This Lock Context is readonly!");
        }
        LockType oldLT = getExplicitLockType(transaction);
        if (oldLT == LockType.NL) {
            throw new NoLockHeldException("Error: No Lock!");
        }
        // escalate lock and descendants to be released
        LockType escLT = LockType.S;
        List<ResourceName> releaseDesc = new ArrayList<>();
        for (Lock l : lockman.getLocks(transaction)) {
            if (l.name == this.name) {
                // if lock is same as this
                // add to list to release this.name later
                releaseDesc.add(this.name);
                if (l.lockType == LockType.SIX || l.lockType == LockType.IX) {
                    escLT = LockType.X;
                } else if (l.lockType == LockType.S || l.lockType == LockType.X) {
                    // return w/o mutating calls for repeat escalate
                    return;
                }
            } else if (l.name.isDescendantOf(this.name)) {
                // if lock is descendant of this
                // add to list to release l.name later
                releaseDesc.add(l.name);
                LockType childLT = l.lockType;
                if (childLT == LockType.IX || childLT == LockType.X || childLT == LockType.SIX) {
                    escLT = LockType.X;
                }
            }
        }
        // escalate lock type != old lock type and is valid substitution
        if (escLT != oldLT && LockType.substitutable(escLT, oldLT)) {
            lockman.acquireAndRelease(transaction, this.name, escLT, releaseDesc);
        }
        // update numChildLocks
        for (ResourceName rName: releaseDesc) {
            LockContext currLC = LockContext.fromResourceName(lockman, rName);
            currLC.numChildLocks.put(transaction.getTransNum(), 0);
        }
        // return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return this.lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType explicitLT = getExplicitLockType(transaction);
        if (explicitLT != LockType.NL) {
            return explicitLT;
        }
        // iterate through parentContexts
        LockContext currContext = parentContext();
        while (currContext != null && explicitLT == LockType.NL) {
            explicitLT = currContext.getExplicitLockType(transaction);
            currContext = currContext.parentContext();
        }
        if (explicitLT == LockType.IS || explicitLT == LockType.IX) {
            return LockType.NL;
        }
        if (explicitLT == LockType.SIX) {
            return LockType.S;
        }
        return explicitLT;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext currPLC = parentContext();
        while (currPLC != null) {
            if (currPLC.getExplicitLockType(transaction) == LockType.SIX) {
                return true;
            }
            currPLC = currPLC.parentContext();
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        // return new ArrayList<>();
        List<ResourceName> sisDescendants = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock l: locks) {
            if (l.name.isDescendantOf(this.name) && (l.lockType == LockType.S || l.lockType == LockType.IS)) {
                sisDescendants.add(l.name);
            }
        }
        return sisDescendants;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

