/*
 * The MIT License
 *
 * Copyright (c) 2009 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package htsjdk.samtools.util;

import htsjdk.samtools.Defaults;
import htsjdk.samtools.SAMException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Collection to which many records can be added.  After all records are added, the collection can be
 * iterated, and the records will be returned in order defined by the comparator.  Records may be spilled
 * to a temporary directory if there are more records added than will fit in memory.  As a result of this,
 * the objects returned may not be identical to the objects added to the collection, but they should be
 * equal as determined by the codec used to write them to disk and read them back.
 *
 * When iterating over the collection, the number of file handles required is numRecordsInCollection/maxRecordsInRam.
 * If this becomes a limiting factor, a file handle cache could be added.
 *
 * If Snappy DLL is available and snappy.disable system property is not set to true, then Snappy is used
 * to compress temporary files.
 */
public class MultithreadSortingCollection<T> extends SortingCollection<T> {
    private static final Log log = Log.getInstance(SortingCollection.class);

    /**
     * Size of ramRecords' pool
     */
    public static final int POOL_CAPACITY = 3;

    /**
     * Signal for sortService, to stop.
     */
    private final PackOfRecords FINISH_SIGNAL = new PackOfRecords(null, 0);

    private final ExecutorService spillService;
    private final ExecutorService sortService;
    private final BlockingQueue<T[]> instancePool;
    private final BlockingQueue<PackOfRecords> unsortedRamRecordsQueue;
    private final BlockingQueue<PackOfRecords> sortedRamRecordQueue;

    /**
     * Prepare to accumulate records to be sorted
     * @param componentType Class of the record to be sorted.  Necessary because of Java generic lameness.
     * @param codec For writing records to file and reading them back into RAM
     * @param comparator Defines output sort order
     * @param maxRecordsInRam how many records to accumulate before spilling to disk
     * @param tmpDir Where to write files of records that will not fit in RAM
     */
    public MultithreadSortingCollection(final Class<T> componentType, final SortingCollection.Codec<T> codec,
                                        final Comparator<T> comparator, final int maxRecordsInRam, final File... tmpDir) {
        super(componentType, codec, comparator, maxRecordsInRam, tmpDir);

        instancePool = new LinkedBlockingQueue<T[]>(POOL_CAPACITY);
        for (int i = 0; i < POOL_CAPACITY - 1; i++){
            instancePool.offer((T[]) Array.newInstance(componentType, maxRecordsInRam));
        }

        unsortedRamRecordsQueue = new LinkedBlockingQueue<PackOfRecords>(POOL_CAPACITY);
        sortedRamRecordQueue = new LinkedBlockingQueue<PackOfRecords>(POOL_CAPACITY);

        sortService = Executors.newSingleThreadExecutor();
        sortService.submit(new Sorting(comparator));

        spillService = Executors.newSingleThreadExecutor();
        spillService.submit(new SpillingManager(codec));
    }

    private class PackOfRecords {
        private T[] pack;
        private int packSize;

        public PackOfRecords(T[] pack, int packSize){
            this.pack = pack;
            this.packSize = packSize;
        }

        public boolean isSignalForFinish(){
            return pack == null && packSize == 0;
        }
    }

    @Override
    public void add(final T rec) {
        try {
            if (doneAdding) {
                throw new IllegalStateException("Cannot add after calling doneAdding()");
            }
            if (iterationStarted) {
                throw new IllegalStateException("Cannot add after calling iterator()");
            }

            if (numRecordsInRam == maxRecordsInRam) {
                final T[] buffRamRecords;

                buffRamRecords = this.ramRecords;
                final int buffNumRecordsInRam = this.numRecordsInRam;
                this.ramRecords = instancePool.take();
                this.numRecordsInRam = 0;

                unsortedRamRecordsQueue.put(new PackOfRecords(buffRamRecords, buffNumRecordsInRam));
            }
            ramRecords[numRecordsInRam++] = rec;
        } catch (InterruptedException e) {
            shutdownServices();

            // Restore the interrupted status.
            Thread.currentThread().interrupt();

            throw new SAMException("Failed to add() MultithreadSortingCollection", e);
        }
    }

    private void shutdownServices() {
        if (!spillService.isTerminated()) {
            spillService.shutdownNow();
        }

        if (!sortService.isTerminated()) {
            sortService.shutdownNow();
        }
    }

    /**
     * This method can be called after caller is done adding to collection, in order to possibly free
     * up memory.  If iterator() is called immediately after caller is done adding, this is not necessary,
     * because iterator() triggers the same freeing.
     */
    public void doneAdding() {
        try {
            if (this.cleanedUp) {
                throw new IllegalStateException("Cannot call doneAdding() after cleanup() was called.");
            }
            if (doneAdding) {
                return;
            }

            doneAdding = true;

            this.unsortedRamRecordsQueue.put(FINISH_SIGNAL);
            sortService.shutdown();
            sortService.awaitTermination(1, TimeUnit.DAYS);
            spillService.shutdown();
            spillService.awaitTermination(1, TimeUnit.DAYS);

            if (this.files.isEmpty()) {
                return;
            }

            if (this.numRecordsInRam > 0) {
                Arrays.sort(this.ramRecords, 0, this.numRecordsInRam, this.comparator);
                spillToDisk(this.ramRecords, this.numRecordsInRam, this.codec);
            }
        } catch (InterruptedException e) {
            shutdownServices();

            // Restore the interrupted status.
            Thread.currentThread().interrupt();

            throw new SAMException("Failed to add() MultithreadSortingCollection", e);
        }

        // Facilitate GC
        this.ramRecords = null;
        if (instancePool != null && !instancePool.isEmpty()){
            instancePool.clear();
        }
    }

    /**
     * Sort the records in memory, write them to a file, and clear the buffer of records in memory.
     */
    private void spillToDisk(T[] buffRamRecords, int buffNumRecordsInRam, Codec<T> codec) throws InterruptedException {

        try {
            final File f = newTempFile();
            OutputStream os = null;
            try {
                os = tempStreamFactory.wrapTempOutputStream(new FileOutputStream(f), Defaults.BUFFER_SIZE);
                codec.setOutputStream(os);
                for (int i = 0; i < buffNumRecordsInRam; ++i) {
                    codec.encode(buffRamRecords[i]);
                    // Facilitate GC
                    buffRamRecords[i] = null;
                }

                os.flush();
            } catch (RuntimeIOException ex) {
                throw new RuntimeIOException("Problem writing temporary file " + f.getAbsolutePath() +
                        ".  Try setting TMP_DIR to a file system with lots of space.", ex);
            } finally {
                if (os != null) {
                    os.close();
                }
            }

            this.files.add(f);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public void cleanup() {
        this.iterationStarted = true;
        this.cleanedUp = true;

        IOUtil.deleteFiles(this.files);
    }

    /**
     * Syntactic sugar around the ctor, to save some typing of type parameters
     *
     * @param componentType Class of the record to be sorted.  Necessary because of Java generic lameness.
     * @param codec For writing records to file and reading them back into RAM
     * @param comparator Defines output sort order
     * @param maxRecordsInRAM how many records to accumulate in memory before spilling to disk
     * @param tmpDir Where to write files of records that will not fit in RAM
     */
    public static <T> MultithreadSortingCollection<T> newInstance(final Class<T> componentType,
                                                                  final SortingCollection.Codec<T> codec,
                                                                  final Comparator<T> comparator,
                                                                  final int maxRecordsInRAM,
                                                                  final File... tmpDir) {
        return new MultithreadSortingCollection<T>(componentType, codec, comparator, maxRecordsInRAM/POOL_CAPACITY/2,
                tmpDir);

    }

    /**
     * Syntactic sugar around the ctor, to save some typing of type parameters
     *
     * @param componentType Class of the record to be sorted.  Necessary because of Java generic lameness.
     * @param codec For writing records to file and reading them back into RAM
     * @param comparator Defines output sort order
     * @param maxRecordsInRAM how many records to accumulate in memory before spilling to disk
     * @param tmpDirs Where to write files of records that will not fit in RAM
     */
    public static <T> MultithreadSortingCollection<T> newInstance(final Class<T> componentType,
                                                                  final SortingCollection.Codec<T> codec,
                                                                  final Comparator<T> comparator,
                                                                  final int maxRecordsInRAM,
                                                                  final Collection<File> tmpDirs) {
        return new MultithreadSortingCollection<T>(componentType,
                codec,
                comparator,
                maxRecordsInRAM/POOL_CAPACITY/2,
                tmpDirs.toArray(new File[tmpDirs.size()]));

    }


    /**
     * Syntactic sugar around the ctor, to save some typing of type parameters.  Writes files to java.io.tmpdir
     *
     * @param componentType Class of the record to be sorted.  Necessary because of Java generic lameness.
     * @param codec For writing records to file and reading them back into RAM
     * @param comparator Defines output sort order
     * @param maxRecordsInRAM how many records to accumulate in memory before spilling to disk
     */
    public static <T> MultithreadSortingCollection<T> newInstance(final Class<T> componentType,
                                                                  final SortingCollection.Codec<T> codec,
                                                                  final Comparator<T> comparator,
                                                                  final int maxRecordsInRAM) {

        final File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        return new MultithreadSortingCollection<T>(componentType, codec, comparator, maxRecordsInRAM/POOL_CAPACITY/2,
                tmpDir);
    }

    private class SpillingManager implements Runnable {

        SortingCollection.Codec<T> codec;

        public SpillingManager(SortingCollection.Codec<T> codec) {
            this.codec = codec.clone();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    PackOfRecords pack = sortedRamRecordQueue.take();

                    if (pack.isSignalForFinish()) {
                        return;
                    }

                    spillToDisk(pack.pack, pack.packSize, codec);
                    instancePool.put(pack.pack);
                } catch (InterruptedException e) {
                    log.error("Interrupted in spilling task", e);
                    break;
                }
            }
        }
    }

    private class Sorting implements Runnable {
        private final Comparator<T> comparator;

        public Sorting(Comparator<T> comparator) {
            this.comparator = comparator;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    PackOfRecords pack = unsortedRamRecordsQueue.take();

                    if (pack.isSignalForFinish()) {
                        sortedRamRecordQueue.put(pack);
                        return;
                    }

                    Arrays.sort(pack.pack, 0, pack.packSize, comparator);
                    sortedRamRecordQueue.put(pack);
                } catch (InterruptedException e) {
                    log.error("Interrupted in sorting task", e);
                    break;
                }
            }
        }
    }
}
