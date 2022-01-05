package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses);
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement
        Run sortedRun = new Run(this.transaction, getSchema());

        List<Record> recordList = new ArrayList<>();
        while (records.hasNext()) {
            recordList.add(records.next());
        }
        recordList.sort(this.comparator);

        sortedRun.addAll(recordList);
        return sortedRun;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        // TODO(proj3_part1): implement
        // run to return
        Run mergedRun = new Run(this.transaction, getSchema());
        // priority queue to determine order of adding records
        RecordPairComparator comp = new RecordPairComparator();
        PriorityQueue<Pair<Record, Integer>> queue = new PriorityQueue<Pair<Record, Integer>>(runs.size(), comp);
        // list of iterators for each run in order
        List<Iterator<Record>> recIter = new ArrayList<>();

        // adding a placeholder
        // queue.add(new Pair<>(new Record(), 0));

        for (int i = 0; i < runs.size(); i++) {
            // creating iterators for each run
            Iterator<Record> iter = runs.get(i).iterator();
            recIter.add(iter);

            // grab the current smallest record in each run in order if there exists one
            if (iter.hasNext()) {
                queue.add(new Pair<>(iter.next(), i));
            }
        }
        // add records in order of the priority queue until there are no more records in the queue to add
        // after adding a record to the new merged run, add that run's next smallest to the queue
        while (!queue.isEmpty()) {
            // add the next record to new merged run
            Pair<Record, Integer> p = queue.remove();
            int runID = p.getSecond();
            mergedRun.add(p.getFirst());

            // check if the run that this record is from has more records
            // if yes add record to queue
            Iterator<Record> iter = recIter.get(runID);
            if (iter.hasNext()) {
                queue.add(new Pair<>(iter.next(), runID));
            }
        }
        return mergedRun;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        List<Run> mergedSortedRuns = new ArrayList<>();
        int N = runs.size();
        // max # runs to merge at a time
        int max = numBuffers - 1;
        // run index
        int i = 0;
        while (i < N) {
            // if N is not a perfect multiple of (numBuffers - 1) -> i ~ runs.size() for the last
            int rightEnd = Math.min(i + max, runs.size());
            mergedSortedRuns.add(mergeSortedRuns(runs.subList(i, rightEnd)));
            i = rightEnd;
        }
        return mergedSortedRuns;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();

        // TODO(proj3_part1): implement
        // methods to use
        // 1. sortRun: sortRun(get iter for this section of records), sort each run and add to list of sorted runs
        // public Run sortRun(*Iterator<Record> records*)
        // 2. mergePass: mergePass(list of sorted runs), executes one pass, continue until we end with one single run
        // List<Run> mergePass(List<Run> runs)

        // list of sorted runs
        List<Run> runs = new ArrayList<>();

        // pass 0
        while(sourceIterator.hasNext()) {
            // pass 0 -> uses B (numBuffers) for maxPages
            BacktrackingIterator<Record> recordsIter = QueryOperator.getBlockIterator(
                    sourceIterator, getSchema(), numBuffers);
            // sort run and add to list
            Run sortedRun = sortRun(recordsIter);
            runs.add(sortedRun);
        }

        // rest of the passes, continue merging until we reach one single run
        // accounts for if we are done after pass 0
        while (runs.size() > 1)  {
            // updates with result of one pass
            runs = mergePass(runs);
        }

        // runs should only have one element now
        return runs.get(0); // TODO(proj3_part1): replace this!
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

