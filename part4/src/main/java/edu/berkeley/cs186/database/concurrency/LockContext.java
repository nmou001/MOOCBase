package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
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
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
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
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
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

        if (readonly) {
            throw new UnsupportedOperationException("Context is readonly");
        } else if (getExplicitLockType(transaction) != LockType.NL) {
            throw new DuplicateLockRequestException("Duplicate lock");
        } else if (parent != null) {
            LockType parentLock = parent.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(parentLock, lockType))
                throw new InvalidLockException("Request is invalid");
            if (hasSIXAncestor(transaction) && (lockType == LockType.IS || lockType == LockType.S))
                throw new InvalidLockException("Can't acquire IS/S lock with SIX ancestor");
        }

        lockman.acquire(transaction, name, lockType);

        if (parent == null)
            return;

        int currNumLocks = parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0);
        parent.numChildLocks.put(transaction.getTransNum(), currNumLocks + 1);
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
        if (readonly) throw new UnsupportedOperationException("context is readonly");
        if (getExplicitLockType(transaction) == LockType.NL) {
            throw new NoLockHeldException("no lock on `name` is held by `transaction`");
        }
        // Check if child holds a concrete lock
        for (Lock lock: lockman.getLocks(transaction)) {
            if (lock.name.isDescendantOf(name) && lock.lockType != LockType.NL) {
                throw new InvalidLockException("lock cannot be released w/o violating MG constraints");
            }
        }
        lockman.release(transaction, name);

        if (parent != null) {
            int currNum = parent.numChildLocks.get(transaction.getTransNum());
            parent.numChildLocks.put(transaction.getTransNum(), currNum - 1);
        }
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
            throw new UnsupportedOperationException("context is readonly");
        } else if (getExplicitLockType(transaction).equals(newLockType)) {
            throw new DuplicateLockRequestException("Xact already has 'newLockType' lock");
        } else if (getExplicitLockType(transaction).equals(LockType.NL)) {
            throw new NoLockHeldException("Xact does not have lock");
        } else if (newLockType == LockType.SIX && hasSIXAncestor(transaction)) {
            throw new InvalidLockException("Can't grant SIX, already have SIX");
        } else if (newLockType.toString().equals("SIX")
                && (getExplicitLockType(transaction) == LockType.S
                || getExplicitLockType(transaction) == LockType.IS
                || getExplicitLockType(transaction) == LockType.IX)) {
            List<ResourceName> toRelease = sisDescendants(transaction);
            toRelease.add(name);
            lockman.acquireAndRelease(transaction, name, LockType.SIX, toRelease);
            return;
        }
        lockman.promote(transaction, name, newLockType);

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
        if (readonly) throw new UnsupportedOperationException("context is readonly");
        LockType currLockType = getExplicitLockType(transaction);

        int level = 0;
        if (currLockType == LockType.X || currLockType == LockType.S){
            return;
        } else if (currLockType == LockType.IS) {
            level = 1;
        } else if (currLockType == LockType.IX || currLockType == LockType.SIX) {
            level = 2;
        } else if (currLockType == LockType.NL) {
            throw new NoLockHeldException("No lock held");
        }

        List<ResourceName> toRelease = new ArrayList<>();
        for (Lock lock: lockman.getLocks(transaction)) {
            if (lock.name.isDescendantOf(name)) {
                toRelease.add(lock.name);

                if (level < 2 && lock.lockType == LockType.X) {
                    level = 2;
                } else if (level < 1 && lock.lockType == LockType.S) {
                    level = 1;
                }
            }
        }
        toRelease.add(name);
        LockType coarseLock = LockType.NL;
        if (level == 2) {
            coarseLock = LockType.X;
        } else if (level == 1) {
            coarseLock = LockType.S;
        }

        lockman.acquireAndRelease(transaction, name, coarseLock, toRelease);
        wipeNumChildren();
    }

    private void wipeNumChildren() {
        for (Long key: numChildLocks.keySet()) {
            numChildLocks.put(key, 0);
        }
        for (LockContext child: children.values())
            child.wipeNumChildren();
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType explicit = LockType.NL;
        for (Lock l : lockman.getLocks(transaction)) {
            if (l.name.equals(name)) {
                explicit = l.lockType;
            }
        }
        return explicit;
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
        LockContext curr = this;
        LockType currType = this.getExplicitLockType(transaction);
        while (currType == LockType.NL && curr != null) {
            curr = curr.parent;
            if (curr != null) {
                currType = curr.getExplicitLockType(transaction);
            }
        }
        switch (currType) {
            case SIX:
                return LockType.S;
            case NL:
            case S:
            case X:
                return currType;
            case IS:
            case IX:
                return LockType.NL;
        }
        return currType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        if (parent != null) {
            return parent.getExplicitLockType(transaction) == LockType.SIX || parent.hasSIXAncestor(transaction);
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
        List<ResourceName> sisDesc = new ArrayList<>();
        for (Lock lock: lockman.getLocks(transaction)) {
            if (lock.name.isDescendantOf(name) && (lock.lockType == LockType.IS || lock.lockType == LockType.S)) {
                sisDesc.add(lock.name);
            }
        }
        return sisDesc;
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
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
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

