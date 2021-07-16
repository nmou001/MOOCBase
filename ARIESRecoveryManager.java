package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.*;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5_part1): implement
        TransactionTableEntry t = transactionTable.get(transNum);
        LogRecord r = new CommitTransactionLogRecord(transNum, t.lastLSN);
        logManager.flushToLSN(logManager.appendToLog(r));

        t.transaction.setStatus(Transaction.Status.COMMITTING);
        //might need to update LSN or flushed LSN
        t.lastLSN = logManager.getFlushedLSN();

        return t.lastLSN;

    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5_part1): implement
        TransactionTableEntry t = transactionTable.get(transNum);
        LogRecord r = new AbortTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN);
        t.lastLSN = logManager.appendToLog(r);
        t.transaction.setStatus(Transaction.Status.ABORTING);
        return t.lastLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5_part1): implement
        TransactionTableEntry t = transactionTable.get(transNum);
        if (Transaction.Status.ABORTING == t.transaction.getStatus()) {
            rollbackToLSN(transNum, 0);
        }
        LogRecord r = new EndTransactionLogRecord(transNum, t.lastLSN);

        t.lastLSN = logManager.appendToLog(r);
        t.transaction.setStatus(Transaction.Status.COMPLETE);
        // Remove transaction from table
        transactionTable.remove(transNum);
        return t.lastLSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * The general idea is starting the LSN of the most recent record that hasn't
     * been undone:
     * - while the current LSN is greater than the LSN we're rolling back to
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Flush if necessary
     *       - Update the dirty page table if necessary in the following cases:
     *          - You undo an update page record (this is the same as applying
     *            the original update in reverse, which would dirty the page)
     *          - You undo alloc page page record (note that freed pages are no
     *            longer considered dirty)
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);

        // TODO(proj5_part1) implement the rollback logic described above

        LogRecord currentRecord;
        while (currentLSN != -1 && currentLSN > LSN) {
            currentRecord =  logManager.fetchLogRecord(currentLSN);
            if (currentRecord.isUndoable()) {
                // TODO: maybe replace currentLSN with lastRecordLSN or transactionEntry.lastLSN?
                Pair<LogRecord, Boolean> clrPair = currentRecord.undo(transactionEntry.lastLSN);
                LogRecord clr = clrPair.getFirst();
                Boolean needsFlush = clrPair.getSecond();

                // TODO: maybe replace lastLSN to be whatever was before clr? or to currentLSN?
                transactionEntry.lastLSN = logManager.appendToLog(clr);
                if (needsFlush) {
                    logManager.flushToLSN(transactionEntry.lastLSN);
                }

                if (clr.type == LogType.UNDO_UPDATE_PAGE) {
                    Long pageNum = clr.getPageNum().get();
                    if (!dirtyPageTable.containsKey(pageNum))
                        dirtyPageTable.put(pageNum, clr.LSN);
                } else if (clr.type == LogType.UNDO_ALLOC_PAGE) {
                    Long pageNum = clr.getPageNum().get();
                    dirtyPageTable.remove(pageNum);
                }

                clr.redo(diskSpaceManager, bufferManager);
            }
            // Get next record to undo, otherwise set to null & stop iteration
            Optional<Long> optRecordLSN = currentRecord.getUndoNextLSN();
            if (optRecordLSN.isPresent()) {
                currentLSN = optRecordLSN.get();
            } else {
                Optional<Long> nextLSN = currentRecord.getPrevLSN();
                if (nextLSN.isPresent()) {
                    currentLSN = nextLSN.get();
                } else
                    currentLSN = -1;
            }
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);

        // TODO(proj5_part1): implement
        TransactionTableEntry t = transactionTable.get(transNum);
        t.touchedPages.add(pageNum);
        long prevLSN = t.lastLSN;
        if (before.length > BufferManager.EFFECTIVE_PAGE_SIZE / 2) {
            byte[] empty = null;
            LogRecord undo = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, empty);
            LogRecord redo = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, empty, after);
            logManager.appendToLog(undo);
            t.lastLSN = logManager.appendToLog(redo);

        } else {
            LogRecord standard = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
            t.lastLSN = logManager.appendToLog(standard);
        }
        if (!dirtyPageTable.containsKey(pageNum)) {
            dirtyPageTable.put(pageNum, t.lastLSN);

        }
        return t.lastLSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);


        // TODO(proj5_part1): implement
        rollbackToLSN(transNum, LSN);
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(proj5_part1): generate end checkpoint record(s) for DPT and transaction table
        dpt.putAll(dirtyPageTable);

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            txnTable.put(entry.getKey(), new Pair(entry.getValue().transaction.getStatus(), entry.getValue().lastLSN));
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                            dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                            dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.cleanDPT();

        return () -> {
            this.restartUndo();
            this.checkpoint();
        };
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
         * If the log record is for a change in transaction status:
         * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
         * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
         * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     * - The status's in the transaction table should be updated if its possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> complete is a possible transition,
     *   but committing -> running is not.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        // TODO(proj5_part2): implement
        for (Iterator<LogRecord> it = logManager.scanFrom(LSN); it.hasNext(); ) {
            LogRecord logRecord = it.next();
            // Transaction related log
            if (logRecord.getTransNum().isPresent()) {
                // Get or create transEntry & trans
                long transNum = logRecord.getTransNum().get();
                TransactionTableEntry transactionTableEntry = getOrAddEntry(transNum);
                Transaction transaction = transactionTableEntry.transaction;

                transactionTableEntry.lastLSN = logRecord.getLSN();

                // Page updates
                updateTransactionPagesIfNeeded(logRecord, transactionTableEntry, logRecord.getLSN());

                // Status updates
                updateTransactionStatusIfNeeded(logRecord, transaction);
            }
            if (logRecord.type == LogType.BEGIN_CHECKPOINT) {
                // TODO: may need to loop through all the transaction to update all transaction counters (?)
                processBeginCheckpointRecord((BeginCheckpointLogRecord) logRecord);
            } else if (logRecord.type == LogType.END_CHECKPOINT) {
                processEndCheckpointRecord((EndCheckpointLogRecord) logRecord);
            }
        }
        endAnalysisTransactions();
    }

    private TransactionTableEntry getOrAddEntry(long transNum) {
        TransactionTableEntry transactionTableEntry;
        if (!transactionTable.containsKey(transNum)) {
            Transaction transaction = newTransaction.apply(transNum);
            transaction.setStatus(Transaction.Status.RUNNING);
            transactionTableEntry = new TransactionTableEntry(transaction);
            transactionTable.put(transNum, transactionTableEntry);
        } else {
            transactionTableEntry = transactionTable.get(transNum);
        }
        return transactionTableEntry;
    }

    private void updateTransactionPagesIfNeeded(LogRecord logRecord,
                                                TransactionTableEntry transactionTableEntry,
                                                long recLSN) {
        if (logRecord.getPageNum().isPresent()) {
            long pageNum = logRecord.getPageNum().get();
            transactionTableEntry.touchedPages.add(pageNum);
            acquireTransactionLock(
                    transactionTableEntry.transaction,
                    getPageLockContext(pageNum),
                    LockType.X);

            if (logRecord.type == LogType.UPDATE_PAGE ||
                logRecord.type == LogType.UNDO_UPDATE_PAGE) {
                if (!dirtyPageTable.containsKey(pageNum))
                    dirtyPageTable.put(pageNum, recLSN);

            } else if (logRecord.type == LogType.FREE_PAGE ||
                    logRecord.type == LogType.UNDO_FREE_PAGE) {
                dirtyPageTable.remove(pageNum);
            }
        }
    }

    private void updateTransactionStatusIfNeeded(LogRecord logRecord, Transaction transaction) {
        if (logRecord.type == LogType.COMMIT_TRANSACTION) {
            transaction.setStatus(Transaction.Status.COMMITTING);
        } else if (logRecord.type == LogType.END_TRANSACTION) {
            transaction.cleanup();
            transaction.setStatus(Transaction.Status.COMPLETE);
        } else if (logRecord.type == LogType.ABORT_TRANSACTION) {
            transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        }
    }

    private void processEndCheckpointRecord(EndCheckpointLogRecord endRecord) {
        for (Map.Entry<Long, Long> kv: endRecord.getDirtyPageTable().entrySet()) {
            dirtyPageTable.put(kv.getKey(), kv.getValue());
        }

        for (Map.Entry<Long, Pair<Transaction.Status, Long>> kv: endRecord.getTransactionTable().entrySet()) {
            Long transNum = kv.getKey();
            Long lastLSN = kv.getValue().getSecond();
            Transaction.Status status = kv.getValue().getFirst();
            TransactionTableEntry tableEntry = getOrAddEntry(transNum);
            if (tableEntry.lastLSN < lastLSN)
                tableEntry.lastLSN = lastLSN;

            Transaction.Status currStatus = tableEntry.transaction.getStatus();
            if (currStatus == Transaction.Status.RUNNING) {
                tableEntry.transaction.setStatus(
                        status == Transaction.Status.ABORTING ? Transaction.Status.RECOVERY_ABORTING : status
                );
            } else if ((currStatus == Transaction.Status.ABORTING || currStatus == Transaction.Status.RECOVERY_ABORTING)
                    && status == Transaction.Status.COMPLETE) {
                tableEntry.transaction.setStatus(status);
            } else if (currStatus == Transaction.Status.COMMITTING && status == Transaction.Status.COMPLETE) {
                tableEntry.transaction.setStatus(status);
            }

            if (currStatus != Transaction.Status.COMPLETE) {
                for (Long num: endRecord.getTransactionTouchedPages().getOrDefault(transNum, new ArrayList<>())) {
                    acquireTransactionLock(tableEntry.transaction, getPageLockContext(num), LockType.X);
                    tableEntry.touchedPages.add(num);
                }
            }
        }


    }

    private void processBeginCheckpointRecord(BeginCheckpointLogRecord beginRecord) {
        if (beginRecord.getMaxTransactionNum().isPresent())
            if (beginRecord.getMaxTransactionNum().get() > getTransactionCounter.get())
                updateTransactionCounter.accept(beginRecord.getMaxTransactionNum().get());
    }

    private void endAnalysisTransactions() {
        Set<Long> toRemove = new HashSet<>();
        for (Map.Entry<Long, TransactionTableEntry> kv: transactionTable.entrySet()) {
            TransactionTableEntry transactionTableEntry = kv.getValue();
            Optional<Long> removeKey = endTransaction(kv.getKey(), kv.getValue());
            if (removeKey.isPresent())
                toRemove.add(removeKey.get());
        }

        // Remove aborted and completed from transaction table
        for (Long key: toRemove) {
            transactionTable.remove(key);
        }
    }

    private Optional<Long> endTransaction(long transNum, TransactionTableEntry tableEntry) {
        Transaction.Status status = tableEntry.transaction.getStatus();
        if (status == Transaction.Status.RUNNING) {
            abort(transNum);
            tableEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        } else if (status == Transaction.Status.COMMITTING) {
            tableEntry.transaction.cleanup();
            end(transNum);
            tableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
            return Optional.of(transNum);
        } else if (status == Transaction.Status.COMPLETE) {
            return Optional.of(transNum);
        }
        return Optional.empty();
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(proj5_part2): implement
        Iterator<LogRecord> redoiter = logManager.scanFrom(0);
        while (redoiter.hasNext()) {
            LogRecord logRecord = redoiter.next();
            if (logRecord.isRedoable()) {
                if ((logRecord.getType().equals(LogType.UPDATE_PAGE) || logRecord.getType().equals(LogType.ALLOC_PAGE)
                        || logRecord.getType().equals(LogType.FREE_PAGE) || logRecord.getType().equals(LogType.UNDO_ALLOC_PAGE))){
                    long dirty = logRecord.getPageNum().get();
                    long currLSN = logRecord.LSN;
                    if (currLSN > bufferManager.fetchPage(getPageLockContext(dirty), dirty, true).getPageLSN()) {
                        logRecord.redo(diskSpaceManager, bufferManager);
                    }
                }
                if (logRecord.getType().equals(LogType.ALLOC_PART) || logRecord.getType().equals(LogType.FREE_PAGE)) {
                    logRecord.redo(diskSpaceManager, bufferManager);
                }
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(proj5_part2): implement
        PriorityQueue<Long> undoPQ = new PriorityQueue<>(Collections.reverseOrder());

        // Get the aborting transaction's last LSN
        for (Map.Entry<Long, TransactionTableEntry> kv: transactionTable.entrySet()) {
            TransactionTableEntry transactionTableEntry = kv.getValue();
            Transaction.Status status = transactionTableEntry.transaction.getStatus();
            if (status == Transaction.Status.RECOVERY_ABORTING) {
                undoPQ.add(transactionTableEntry.lastLSN);
            }
        }

        while (!undoPQ.isEmpty()) {
            long lsn = undoPQ.poll();
            LogRecord logRecord = logManager.fetchLogRecord(lsn);

            if (logRecord.isUndoable()) {
                TransactionTableEntry tableEntry = transactionTable.get(logRecord.getTransNum().get());
                Pair<LogRecord, Boolean> pair = logRecord.undo(tableEntry.lastLSN);
                LogRecord clrRecord = pair.getFirst();
                Boolean toFlush = pair.getSecond();

                long newLSN = logManager.appendToLog(clrRecord);
                updateTransactionPagesIfNeeded(clrRecord, tableEntry, newLSN);

                if (toFlush)
                    logManager.flushToLSN(newLSN);

                tableEntry.lastLSN = newLSN;
                clrRecord.redo(diskSpaceManager, bufferManager);

            }
            long nextLSN = -1;
            if (logRecord.getUndoNextLSN().isPresent())
                nextLSN = logRecord.getUndoNextLSN().get();
            else if (logRecord.getPrevLSN().isPresent())
                nextLSN = logRecord.getPrevLSN().get();

            if (nextLSN == 0)
                endAndRemoveTransaction(logRecord);
            else if (nextLSN != -1)
                undoPQ.add(nextLSN);
        }
    }

    private void endAndRemoveTransaction(LogRecord logRecord) {
        if (logRecord.getTransNum().isPresent()) {
            long transNum = logRecord.getTransNum().get();
            end(transNum);
            transactionTable.remove(transNum);
        }
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager. THIS IS SLOW
     * and should only be used during recovery.
     */
    private void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                        lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}