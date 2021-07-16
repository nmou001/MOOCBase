package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.ConcatBacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

import javax.xml.crypto.Data;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            //variable name: sorting left, type: sort operator
            //variable name: sorting right, type: sort operator
            //set left and right Iterators to


            // Hint: you may find the helper methods getTransaction() and getRecordIterator(tableName)
            // in JoinOperator useful here.

            marked = false;

            // Setup sorters
            SortOperator rightSorter = new SortOperator(getTransaction(), this.getRightTableName(),
                                                            new RightRecordComparator());
            SortOperator leftSorter = new SortOperator(getTransaction(), this.getLeftTableName(),
                                                            new LeftRecordComparator());
            // Create backtracking iterators from sorters
            this.rightIterator = getRecordIterator(rightSorter.sort());
            this.leftIterator = getRecordIterator(leftSorter.sort());

            this.nextRecord = null;

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            // We mark the first record so we can reset to it when we advance the left record.
            if (rightRecord != null) {
                rightIterator.markPrev();
            } else { return; }

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        private void fetchNextRecord() {
            this.nextRecord = null;

            do {
                while (rightRecord == null) {
                    advanceLeft();
                    advanceRight();
                }

                DataBox leftval = leftRecord.getValues().get(getLeftColumnIndex());
                DataBox rightval = rightRecord.getValues().get(getRightColumnIndex());

                if (!marked) {
                    // r < s
                    while (leftval.compareTo(rightval) < 0) {
                        advanceLeft();
                        leftval = leftRecord.getValues().get(getLeftColumnIndex());
                    }
                    // r > s
                    while (leftval.compareTo(rightval) > 0) {
                        advanceRight();
                        if (rightRecord == null) {
                            continue;
                        }
                        rightval = rightRecord.getValues().get(getRightColumnIndex());
                    }
                    marked = true;
                    // mark s (previous value spit out)
                    rightIterator.markPrev();
//                    System.out.println("Left value: " + leftval.toString());
//                    System.out.println("Right value: " + rightval.toString());
                }

                if (leftval.compareTo(rightval) == 0) {
//                    System.out.println("Joining!");
                    this.nextRecord = joinRecords(leftRecord, rightRecord);
                    advanceRight();
                } else {
//                    System.out.println("Reset!");
                    rightIterator.reset();
                    advanceRight();
                    advanceLeft();
                    marked = false;
                }

            } while (!hasNext());
        }

        private void advanceLeft() {
            if (!leftIterator.hasNext()) { throw new NoSuchElementException("All Done!"); }
            leftRecord = leftIterator.next();
        }

        private void advanceRight() {
            if (!rightIterator.hasNext()) {
                rightIterator.reset();
                rightRecord = null;
            } else {
                rightRecord = rightIterator.next();
            }
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(proj3_part1): implement

            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(proj3_part1): implement
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }
    }
}
