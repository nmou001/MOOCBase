package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

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
     * - None of the above
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null | lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        if (LockType.substitutable(effectiveLockType, requestType))
            return;

        // Substitutable: X-S, X-X, SIX-S, S-S
        if (explicitLockType == LockType.S && requestType == LockType.X) {          // S-X
            setupAncestor(parentContext, LockType.IX);
            acquireOrPromote(lockContext, transaction, requestType);

        } else if (explicitLockType == LockType.IS && requestType == LockType.X) {  // IS-X
            setupAncestor(parentContext, requestType);
            lockContext.escalate(transaction);
            acquireOrPromote(lockContext, transaction, requestType);

        } else if (explicitLockType == LockType.IS && requestType == LockType.S) {  // IS-S
            lockContext.escalate(transaction);

        } else if (explicitLockType == LockType.IX && requestType == LockType.X) {  // IX-X
            lockContext.promote(transaction, requestType);

        } else if (explicitLockType == LockType.IX && requestType == LockType.S) {  // IS-S
            lockContext.promote(transaction, LockType.SIX);

        } else if (explicitLockType == LockType.SIX && requestType == LockType.X) { // SIX-X
            lockContext.escalate(transaction);
            acquireOrPromote(lockContext, transaction, requestType);

        } else if (explicitLockType == LockType.NL && requestType == LockType.X) {  // NL-X
            setupAncestor(parentContext, LockType.IX);
            lockContext.acquire(transaction, requestType);

        } else if (explicitLockType == LockType.NL && requestType == LockType.S) {  // NL-S
            // If parent has permissions, we grant & dip
            setupAncestor(parentContext, LockType.IS);
            lockContext.acquire(transaction, requestType);

        }

    }

    // TODO(proj4_part2) add any helper methods you want
    // explicit SIX, req S
    // ancestor SIX, needs IS
    // we promote(S)
    private static void setupAncestor(LockContext ancestor, LockType needs) {
        // If we get to root, return immediately
        if (ancestor == null) return;
        TransactionContext transaction = TransactionContext.getTransaction();
        LockType ancestorType = ancestor.getExplicitLockType(transaction);
        // If ancestor can already fulfill obligations, return immediately
        if (LockType.substitutable(ancestorType, needs)) return;

        setupAncestor(ancestor.parentContext(), needs);

        ancestorType = ancestor.getExplicitLockType(transaction);
        if (ancestorType == LockType.NL) {
            ancestor.acquire(transaction, needs);
        } else {
            ancestor.promote(transaction, needs);
        }
    }

    private static void acquireOrPromote(LockContext lockContext, TransactionContext trans, LockType toGet) {
        if (lockContext.getExplicitLockType(trans) == LockType.NL) {
            lockContext.acquire(trans, toGet);
        } else {
            lockContext.promote(trans, toGet);
        }
    }

//    private static LockType computeParentLockType(LockContext parent, LockType currType) {
//
//    }
}
